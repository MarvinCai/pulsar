/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service.persistent;

import com.google.common.base.MoreObjects;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ClearBacklogCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ConcurrentFindCursorPositionException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.InvalidCursorPositionException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerServiceException.PersistenceException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionFencedException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionInvalidCursorPosition;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.utils.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

public class PersistentSubscription implements Subscription {
    protected final PersistentTopic topic;
    protected final ManagedCursor cursor;
    protected volatile Dispatcher dispatcher;
    protected final String topicName;
    protected final String subName;

    private static final int FALSE = 0;
    private static final int TRUE = 1;
    private static final AtomicIntegerFieldUpdater<PersistentSubscription> IS_FENCED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentSubscription.class, "isFenced");
    private volatile int isFenced = FALSE;
    private PersistentMessageExpiryMonitor expiryMonitor;

    // for connected subscriptions, message expiry will be checked if the backlog is greater than this threshold
    private static final int MINIMUM_BACKLOG_FOR_EXPIRY_CHECK = 1000;

    // Map to keep track of message ack by each txn.
    private final ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashSet<Position>> pendingAckMessagesMap;

    // Messages acked by ongoing transaction, pending transaction commit to materialize the acks. For faster look up.
    // Using hashset as a message should only be acked once by one transaction.
    private final ConcurrentOpenHashSet<Position> pendingAckMessages;

    // Message cumulative acked by ongoing transaction, pending transaction commit to materialize the ack.
    // Only one transaction can cumulative ack.
    // This parameter only keep the the largest Position it cumulative ack,as any Position smaller will also be covered.
    private volatile Position pendingCumulativeAckMessage;

    private static final AtomicReferenceFieldUpdater<PersistentSubscription, Position> POSITION_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(PersistentSubscription.class, Position.class,
                    "pendingCumulativeAckMessage");

    // Transaction currently using cumulative ack.
    private volatile TxnID txnID;

    private static final AtomicReferenceFieldUpdater<PersistentSubscription, TxnID> TXNID_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(PersistentSubscription.class, TxnID.class,
                    "txnID");

    public PersistentSubscription(PersistentTopic topic, String subscriptionName, ManagedCursor cursor) {
        this.topic = topic;
        this.cursor = cursor;
        this.topicName = topic.getName();
        this.subName = subscriptionName;
        this.expiryMonitor = new PersistentMessageExpiryMonitor(topicName, subscriptionName, cursor);
        IS_FENCED_UPDATER.set(this, FALSE);
        pendingAckMessages = new ConcurrentOpenHashSet<>();
        pendingAckMessagesMap = new ConcurrentOpenHashMap<>();
    }

    @Override
    public String getName() {
        return this.subName;
    }

    @Override
    public Topic getTopic() {
        return topic;
    }

    @Override
    public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
        cursor.updateLastActive();
        if (IS_FENCED_UPDATER.get(this) == TRUE) {
            log.warn("Attempting to add consumer {} on a fenced subscription", consumer);
            throw new SubscriptionFencedException("Subscription is fenced");
        }

        if (dispatcher == null || !dispatcher.isConsumerConnected()) {
            switch (consumer.subType()) {
            case Exclusive:
                if (dispatcher == null || dispatcher.getType() != SubType.Exclusive) {
                    dispatcher = new PersistentDispatcherSingleActiveConsumer(cursor, SubType.Exclusive, 0, topic);
                }
                break;
            case Shared:
                if (dispatcher == null || dispatcher.getType() != SubType.Shared) {
                    dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursor);
                }
                break;
            case Failover:
                int partitionIndex = TopicName.getPartitionIndex(topicName);
                if (partitionIndex < 0) {
                    // For non partition topics, assume index 0 to pick a predictable consumer
                    partitionIndex = 0;
                }

                if (dispatcher == null || dispatcher.getType() != SubType.Failover) {
                    dispatcher = new PersistentDispatcherSingleActiveConsumer(cursor, SubType.Failover, partitionIndex,
                            topic);
                }
                break;
            case Key_Shared:
                if (dispatcher == null || dispatcher.getType() != SubType.Key_Shared) {
                    dispatcher = new PersistentStickyKeyDispatcherMultipleConsumers(topic, cursor);
                }
                break;
            default:
                throw new ServerMetadataException("Unsupported subscription type");
            }
        } else {
            if (consumer.subType() != dispatcher.getType()) {
                throw new SubscriptionBusyException("Subscription is of different type");
            }
        }

        dispatcher.addConsumer(consumer);
    }

    @Override
    public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
        cursor.updateLastActive();
        if (dispatcher != null) {
            dispatcher.removeConsumer(consumer);
        }
        if (dispatcher.getConsumers().isEmpty()) {
            deactivateCursor();

            if (!cursor.isDurable()) {
                // If cursor is not durable, we need to clean up the subscription as well
                close();
                // when topic closes: it iterates through concurrent-subscription map to close each subscription. so,
                // topic.remove again try to access same map which creates deadlock. so, execute it in different thread.
                topic.getBrokerService().pulsar().getExecutor().submit(() ->{
                    topic.removeSubscription(subName);
                });
            }
        }

        // invalid consumer remove will throw an exception
        // decrement usage is triggered only for valid consumer close
        PersistentTopic.USAGE_COUNT_UPDATER.decrementAndGet(topic);
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] [{}] Removed consumer -- count: {}", topic.getName(), subName, consumer.consumerName(),
                    PersistentTopic.USAGE_COUNT_UPDATER.get(topic));
        }
    }

    public void deactivateCursor() {
        this.cursor.setInactive();
    }

    @Override
    public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {
        dispatcher.consumerFlow(consumer, additionalNumberOfMessages);
    }

    @Override
    public void acknowledgeMessage(List<Position> positions, AckType ackType, Map<String,Long> properties) {
        if (ackType == AckType.Cumulative) {
            if (this.txnID != null) {
                log.warn("[{}][{}] An ongoing transaction is doing cumulative ack, " +
                                "new cumulative ack is not allowed till the transaction is committed.", topicName, subName);
                return;
            }

            if (positions.size() != 1) {
                log.warn("[{}][{}] Invalid cumulative ack received with multiple message ids", topicName, subName);
                return;
            }

            Position position = positions.get(0);
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Cumulative ack on {}", topicName, subName, position);
            }
            cursor.asyncMarkDelete(position, properties, markDeleteCallback, position);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Individual acks on {}", topicName, subName, positions);
            }

            positions.forEach(position -> {
                // If single ack try to ack message in pending_ack status, fail the ack.
                if (this.pendingAckMessages.contains(position)) {
                    log.warn("[{}][{}] Invalid acks position witnin cumulative ack position of an ongoing " +
                                    "transaction:{}{}.", topicName, subName, this.txnID.getMostSigBits(),
                            this.txnID.getLeastSigBits());
                    return;
                }

                // If single ack is within range of cumulative ack of an ongoing transaction, fail the ack.
                if (null != this.pendingCumulativeAckMessage &&
                        ((PositionImpl) position).compareTo((PositionImpl) this.pendingCumulativeAckMessage) < 0) {
                    log.warn("[{}][{}] Invalid acks position witnin cumulative ack position of an ongoing " +
                            "transaction:{}{}.", topicName, subName, this.txnID.getMostSigBits(),
                            this.txnID.getLeastSigBits());
                    return;
                }
            });

            cursor.asyncDelete(positions, deleteCallback, positions);
            dispatcher.getRedeliveryTracker().removeBatch(positions);
        }

        if (topic.getManagedLedger().isTerminated() && cursor.getNumberOfEntriesInBacklog() == 0) {
            // Notify all consumer that the end of topic was reached
            dispatcher.getConsumers().forEach(Consumer::reachedEndOfTopic);
        }
    }

    /**
     * Acknowledge message(s) for an ongoing transaction.
     * <p>
     * It can be of {@link AckType#Individual} or {@link AckType#Cumulative}. Single messages acked by ongoing
     * transaction will be put in pending_ack state and only marked as deleted after transaction is committed.
     * <p>
     * Only one transaction is allowed to do cumulative ack on a subscription at a given time.
     * If a transaction do multiple cumulative ack, only the one with largest position determined by
     * {@link PositionImpl#compareTo(PositionImpl)} will be kept as it cover all position smaller than it.
     * <p>
     * If an ongoing transaction cumulative acked a message and then try to ack single message which is
     * smaller than that one it cumulative acked, it'll succeed.
     * <p>
     * If transaction is aborted all messages acked by it will be put back to pending state.
     *
     * @param txnID                  TransactionID of an ongoing transaction trying to sck message.
     * @param positions              {@link Position}(s) it try to ack.
     * @param ackType                {@link AckType}.
     * @throws TransactionConflictException   Throw InvalidAckException when transaction try to ack message when it shouldn't.
     */
    public void acknowledgeMessage(TxnID txnID, List<Position> positions, AckType ackType) throws TransactionConflictException {
        checkArgument(txnID != null, "TransactionID can not be null.");
        if (AckType.Cumulative == ackType) {
            // Check if another transaction is already using cumulative ack on this subscription.
            if (this.txnID != null && this.txnID != txnID) {
                String errorMsg = String.format("[%s][%s] Transaction:%l%l try to cumulative ack message while" +
                        " transaction:%l%l already cumulative acked messages. ", topicName, subName,
                        this.txnID.getMostSigBits(), this.txnID.getLeastSigBits(), txnID.getMostSigBits(),
                        txnID.getLeastSigBits());
                log.error(errorMsg);
                throw new TransactionConflictException(errorMsg);
            }

            if (positions.size() != 1) {
                String errorMsg = String.format("[%s][%s] Transaction:%l%l, invalid cumulative ack received with multiple " +
                                "message ids", topicName, subName, txnID.getMostSigBits(), txnID.getLeastSigBits());
                log.error(errorMsg);
                throw new IllegalArgumentException(errorMsg);
            }

            Position position = positions.get(0);

            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] TxnID:[{}{}] Cumulative ack on {}", topicName, subName, txnID.getMostSigBits(),
                        txnID.getLeastSigBits(), position);
            }

            synchronized (PersistentSubscription.this) {
                if (this.txnID == null) {
                    // Only set txnID if no transaction is doing cumulative ack.
                    TXNID_UPDATER.set(this, txnID);
                    POSITION_UPDATER.set(this, position);
                } else if (((PositionImpl)position).compareTo((PositionImpl)this.pendingCumulativeAckMessage) > 0) {
                    // If new cumulative ack position is greater than current one, update it.
                    POSITION_UPDATER.set(this, position);
                }
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] TxnID:[{}{}] Individual acks on {}", topicName, subName, txnID.getMostSigBits(),
                        txnID.getLeastSigBits(), positions);
            }

            ConcurrentOpenHashSet<Position> pendingAckMessage =
                    pendingAckMessagesMap.computeIfAbsent(txnID, txn -> new ConcurrentOpenHashSet<>());

            for (Position position : positions) {
                // If try to ack message already acked by some transaction(can be itself), throw exception.
                // Acking single message within range of cumulative ack(if exist) is considered valid operation.
                if (this.pendingAckMessages.contains(position)) {
                    String errorMsg = String.format("[%s][%s]  Transaction:%l%l try to ack message:%s already acked before. ",
                            topicName, subName, txnID.getMostSigBits(), txnID.getLeastSigBits(), position);
                    log.error(errorMsg);
                    throw new TransactionConflictException(errorMsg);
                }

                pendingAckMessage.add(position);
                this.pendingAckMessages.add(position);
            }
        }
    }

    private final MarkDeleteCallback markDeleteCallback = new MarkDeleteCallback() {
        @Override
        public void markDeleteComplete(Object ctx) {
            PositionImpl pos = (PositionImpl) ctx;
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Mark deleted messages until position {}", topicName, subName, pos);
            }
        }

        @Override
        public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
            // TODO: cut consumer connection on markDeleteFailed
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Failed to mark delete for position ", topicName, subName, ctx, exception);
            }
        }
    };

    private final DeleteCallback deleteCallback = new DeleteCallback() {
        @Override
        public void deleteComplete(Object position) {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Deleted message at {}", topicName, subName, position);
            }
        }

        @Override
        public void deleteFailed(ManagedLedgerException exception, Object ctx) {
            log.warn("[{}][{}] Failed to delete message at {}", topicName, subName, ctx, exception);
        }
    };

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("topic", topicName).add("name", subName).toString();
    }

    @Override
    public String getTopicName() {
        return this.topicName;
    }

    @Override
    public SubType getType() {
        return dispatcher != null ? dispatcher.getType() : null;
    }

    @Override
    public String getTypeString() {
        SubType type = getType();
        if (type == null) {
            return "None";
        }

        switch (type) {
        case Exclusive:
            return "Exclusive";
        case Failover:
            return "Failover";
        case Shared:
            return "Shared";
        }

        return "Null";
    }

    @Override
    public CompletableFuture<Void> clearBacklog() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Backlog size before clearing: {}", topicName, subName,
                    cursor.getNumberOfEntriesInBacklog());
        }

        cursor.asyncClearBacklog(new ClearBacklogCallback() {
            @Override
            public void clearBacklogComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Backlog size after clearing: {}", topicName, subName,
                            cursor.getNumberOfEntriesInBacklog());
                }
                future.complete(null);
            }

            @Override
            public void clearBacklogFailed(ManagedLedgerException exception, Object ctx) {
                log.error("[{}][{}] Failed to clear backlog", topicName, subName, exception);
                future.completeExceptionally(exception);
            }
        }, null);

        return future;
    }

    @Override
    public CompletableFuture<Void> skipMessages(int numMessagesToSkip) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Skipping {} messages, current backlog {}", topicName, subName, numMessagesToSkip,
                    cursor.getNumberOfEntriesInBacklog());
        }
        cursor.asyncSkipEntries(numMessagesToSkip, IndividualDeletedEntries.Exclude,
                new AsyncCallbacks.SkipEntriesCallback() {
                    @Override
                    public void skipEntriesComplete(Object ctx) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}][{}] Skipped {} messages, new backlog {}", topicName, subName,
                                    numMessagesToSkip, cursor.getNumberOfEntriesInBacklog());
                        }
                        future.complete(null);
                    }

                    @Override
                    public void skipEntriesFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("[{}][{}] Failed to skip {} messages", topicName, subName, numMessagesToSkip,
                                exception);
                        future.completeExceptionally(exception);
                    }
                }, null);

        return future;
    }

    @Override
    public CompletableFuture<Void> resetCursor(long timestamp) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        PersistentMessageFinder persistentMessageFinder = new PersistentMessageFinder(topicName, cursor);

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Resetting subscription to timestamp {}", topicName, subName, timestamp);
        }
        persistentMessageFinder.findMessages(timestamp, new AsyncCallbacks.FindEntryCallback() {
            @Override
            public void findEntryComplete(Position position, Object ctx) {
                final Position finalPosition;
                if (position == null) {
                    // this should not happen ideally unless a reset is requested for a time
                    // that spans beyond the retention limits (time/size)
                    finalPosition = cursor.getFirstPosition();
                    if (finalPosition == null) {
                        log.warn("[{}][{}] Unable to find position for timestamp {}. Unable to reset cursor to first position",
                                topicName, subName, timestamp);
                        future.completeExceptionally(
                                new SubscriptionInvalidCursorPosition("Unable to find position for specified timestamp"));
                        return;
                    }
                    log.info(
                            "[{}][{}] Unable to find position for timestamp {}. Resetting cursor to first position {} in ledger",
                            topicName, subName, timestamp, finalPosition);
                } else {
                    finalPosition = position;
                }
                resetCursor(finalPosition, future);
            }

            @Override
            public void findEntryFailed(ManagedLedgerException exception, Object ctx) {
                // todo - what can go wrong here that needs to be retried?
                if (exception instanceof ConcurrentFindCursorPositionException) {
                    future.completeExceptionally(new SubscriptionBusyException(exception.getMessage()));
                } else {
                    future.completeExceptionally(new BrokerServiceException(exception));
                }
            }
        });

        return future;
    }

    @Override
    public CompletableFuture<Void> resetCursor(Position position) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        resetCursor(position, future);
        return future;
    }

    private void resetCursor(Position finalPosition, CompletableFuture<Void> future) {
        if (!IS_FENCED_UPDATER.compareAndSet(PersistentSubscription.this, FALSE, TRUE)) {
            future.completeExceptionally(new SubscriptionBusyException("Failed to fence subscription"));
            return;
        }

        final CompletableFuture<Void> disconnectFuture;
        if (dispatcher != null && dispatcher.isConsumerConnected()) {
            disconnectFuture = dispatcher.disconnectAllConsumers();
        } else {
            disconnectFuture = CompletableFuture.completedFuture(null);
        }

        disconnectFuture.whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                log.error("[{}][{}] Failed to disconnect consumer from subscription", topicName, subName, throwable);
                IS_FENCED_UPDATER.set(PersistentSubscription.this, FALSE);
                future.completeExceptionally(
                        new SubscriptionBusyException("Failed to disconnect consumers from subscription"));
                return;
            }
            log.info("[{}][{}] Successfully disconnected consumers from subscription, proceeding with cursor reset",
                    topicName, subName);

            try {
                cursor.asyncResetCursor(finalPosition, new AsyncCallbacks.ResetCursorCallback() {
                    @Override
                    public void resetComplete(Object ctx) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}][{}] Successfully reset subscription to position {}", topicName, subName,
                                    finalPosition);
                        }
                        IS_FENCED_UPDATER.set(PersistentSubscription.this, FALSE);
                        future.complete(null);
                    }

                    @Override
                    public void resetFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("[{}][{}] Failed to reset subscription to position {}", topicName, subName,
                                finalPosition, exception);
                        IS_FENCED_UPDATER.set(PersistentSubscription.this, FALSE);
                        // todo - retry on InvalidCursorPositionException
                        // or should we just ask user to retry one more time?
                        if (exception instanceof InvalidCursorPositionException) {
                            future.completeExceptionally(new SubscriptionInvalidCursorPosition(exception.getMessage()));
                        } else if (exception instanceof ConcurrentFindCursorPositionException) {
                            future.completeExceptionally(new SubscriptionBusyException(exception.getMessage()));
                        } else {
                            future.completeExceptionally(new BrokerServiceException(exception));
                        }
                    }
                });
            } catch (Exception e) {
                log.error("[{}][{}] Error while resetting cursor", topicName, subName, e);
                IS_FENCED_UPDATER.set(PersistentSubscription.this, FALSE);
                future.completeExceptionally(new BrokerServiceException(e));
            }
        });
    }

    @Override
    public CompletableFuture<Entry> peekNthMessage(int messagePosition) {
        CompletableFuture<Entry> future = new CompletableFuture<>();

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Getting message at position {}", topicName, subName, messagePosition);
        }

        cursor.asyncGetNthEntry(messagePosition, IndividualDeletedEntries.Exclude, new ReadEntryCallback() {

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }

            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                future.complete(entry);
            }
        }, null);

        return future;
    }

    @Override
    public long getNumberOfEntriesInBacklog() {
        return cursor.getNumberOfEntriesInBacklog();
    }

    @Override
    public synchronized Dispatcher getDispatcher() {
        return this.dispatcher;
    }

    public long getNumberOfEntriesSinceFirstNotAckedMessage() {
        return cursor.getNumberOfEntriesSinceFirstNotAckedMessage();
    }

    public int getTotalNonContiguousDeletedMessagesRange() {
        return cursor.getTotalNonContiguousDeletedMessagesRange();
    }

    /**
     * Close the cursor ledger for this subscription. Requires that there are no active consumers on the dispatcher
     *
     * @return CompletableFuture indicating the completion of delete operation
     */
    @Override
    public CompletableFuture<Void> close() {
        synchronized (this) {
            if (dispatcher != null && dispatcher.isConsumerConnected()) {
                return FutureUtil.failedFuture(new SubscriptionBusyException("Subscription has active consumers"));
            }
            IS_FENCED_UPDATER.set(this, TRUE);
            log.info("[{}][{}] Successfully closed subscription [{}]", topicName, subName, cursor);
        }

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Disconnect all consumers attached to the dispatcher and close this subscription
     *
     * @return CompletableFuture indicating the completion of disconnect operation
     */
    @Override
    public synchronized CompletableFuture<Void> disconnect() {
        CompletableFuture<Void> disconnectFuture = new CompletableFuture<>();

        // block any further consumers on this subscription
        IS_FENCED_UPDATER.set(this, TRUE);

        (dispatcher != null ? dispatcher.close() : CompletableFuture.completedFuture(null))
                .thenCompose(v -> close()).thenRun(() -> {
                    log.info("[{}][{}] Successfully disconnected and closed subscription", topicName, subName);
                    disconnectFuture.complete(null);
                }).exceptionally(exception -> {
                    IS_FENCED_UPDATER.set(this, FALSE);
                    if (dispatcher != null) {
                        dispatcher.reset();
                    }
                    log.error("[{}][{}] Error disconnecting consumers from subscription", topicName, subName,
                            exception);
                    disconnectFuture.completeExceptionally(exception);
                    return null;
                });

        return disconnectFuture;
    }

    /**
     * Delete the subscription by closing and deleting its managed cursor if no consumers are connected to it. Handle
     * unsubscribe call from admin layer.
     *
     * @return CompletableFuture indicating the completion of delete operation
     */
    @Override
    public CompletableFuture<Void> delete() {
        CompletableFuture<Void> deleteFuture = new CompletableFuture<>();

        log.info("[{}][{}] Unsubscribing", topicName, subName);

        // cursor close handles pending delete (ack) operations
        this.close().thenCompose(v -> topic.unsubscribe(subName)).thenAccept(v -> deleteFuture.complete(null))
                .exceptionally(exception -> {
                    IS_FENCED_UPDATER.set(this, FALSE);
                    log.error("[{}][{}] Error deleting subscription", topicName, subName, exception);
                    deleteFuture.completeExceptionally(exception);
                    return null;
                });

        return deleteFuture;
    }

    /**
     * Handle unsubscribe command from the client API Check with the dispatcher is this consumer can proceed with
     * unsubscribe
     *
     * @param consumer
     *            consumer object that is initiating the unsubscribe operation
     * @return CompletableFuture indicating the completion of ubsubscribe operation
     */
    @Override
    public CompletableFuture<Void> doUnsubscribe(Consumer consumer) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            if (dispatcher.canUnsubscribe(consumer)) {
                consumer.close();
                return delete();
            }
            future.completeExceptionally(
                    new ServerMetadataException("Unconnected or shared consumer attempting to unsubscribe"));
        } catch (BrokerServiceException e) {
            log.warn("Error removing consumer {}", consumer);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CopyOnWriteArrayList<Consumer> getConsumers() {
        Dispatcher dispatcher = this.dispatcher;
        if (dispatcher != null) {
            return dispatcher.getConsumers();
        } else {
            return CopyOnWriteArrayList.empty();
        }
    }

    @Override
    public void expireMessages(int messageTTLInSeconds) {
        if ((getNumberOfEntriesInBacklog() == 0) || (dispatcher != null && dispatcher.isConsumerConnected()
                && getNumberOfEntriesInBacklog() < MINIMUM_BACKLOG_FOR_EXPIRY_CHECK
                && !topic.isOldestMessageExpired(cursor, messageTTLInSeconds))) {
            // don't do anything for almost caught-up connected subscriptions
            return;
        }
        expiryMonitor.expireMessages(messageTTLInSeconds);
    }

    public double getExpiredMessageRate() {
        return expiryMonitor.getMessageExpiryRate();
    }

    public long estimateBacklogSize() {
        return cursor.getEstimatedSizeSinceMarkDeletePosition();
    }

    public SubscriptionStats getStats() {
        SubscriptionStats subStats = new SubscriptionStats();

        Dispatcher dispatcher = this.dispatcher;
        if (dispatcher != null) {
            dispatcher.getConsumers().forEach(consumer -> {
                ConsumerStats consumerStats = consumer.getStats();
                subStats.consumers.add(consumerStats);
                subStats.msgRateOut += consumerStats.msgRateOut;
                subStats.msgThroughputOut += consumerStats.msgThroughputOut;
                subStats.msgRateRedeliver += consumerStats.msgRateRedeliver;
                subStats.unackedMessages += consumerStats.unackedMessages;
            });
        }

        subStats.type = getType();
        if (dispatcher instanceof PersistentDispatcherSingleActiveConsumer) {
            Consumer activeConsumer = ((PersistentDispatcherSingleActiveConsumer) dispatcher).getActiveConsumer();
            if (activeConsumer != null) {
                subStats.activeConsumerName = activeConsumer.consumerName();
            }
        }
        if (SubType.Shared.equals(subStats.type)) {
            if (dispatcher instanceof PersistentDispatcherMultipleConsumers) {
                subStats.unackedMessages = ((PersistentDispatcherMultipleConsumers) dispatcher)
                        .getTotalUnackedMessages();
                subStats.blockedSubscriptionOnUnackedMsgs = ((PersistentDispatcherMultipleConsumers) dispatcher)
                        .isBlockedDispatcherOnUnackedMsgs();
            }
        }
        subStats.msgBacklog = getNumberOfEntriesInBacklog();
        subStats.msgRateExpired = expiryMonitor.getMessageExpiryRate();

        return subStats;
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer) {
        // Check if message is in pending_ack status.

        dispatcher.redeliverUnacknowledgedMessages(consumer);
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {
        // Check if message is in pending_ack status.

        dispatcher.redeliverUnacknowledgedMessages(consumer, positions);
    }

    @Override
    public void addUnAckedMessages(int unAckMessages) {
        dispatcher.addUnAckedMessages(unAckMessages);
    }

    @Override
    public void markTopicWithBatchMessagePublished() {
        topic.markBatchMessagePublished();
    }

    void topicTerminated() {
        if (cursor.getNumberOfEntriesInBacklog() == 0) {
            // notify the consumers if there are consumers connected to this topic.
            if (null != dispatcher) {
                // Immediately notify the consumer that there are no more available messages
                dispatcher.getConsumers().forEach(Consumer::reachedEndOfTopic);
            }
        }
    }

    /**
     * Commit a transaction.
     *
     * @param txnID         {@link TxnID} to identify the transaction.
     * @param properties    Additional user-defined properties that can be associated with a particular cursor position.
     */
    public synchronized void commitTxn(TxnID txnID, Map<String,Long> properties) throws TransactionConflictException {
        if (!this.pendingAckMessagesMap.containsKey(txnID)) {
            String errorMsg = String.format("[%s][%s] Transaction with id:%l%l not found.", topicName, subName,
                    txnID.getMostSigBits(), txnID.getLeastSigBits());
            log.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        // This should never happen.
        if (null != this.pendingCumulativeAckMessage && txnID != this.txnID) {
            String errorMsg = String.format("[%s][%s] Committing transaction:%l%l but another transaction:%l%l" +
                            " cumulative acknowledged.", topicName, subName, txnID.getMostSigBits(),
                    txnID.getLeastSigBits(), this.txnID.getMostSigBits(), this.txnID.getLeastSigBits());
            log.error(errorMsg);
            throw new TransactionConflictException(errorMsg);
        }

        ConcurrentOpenHashSet<Position> pendingAckMessage = this.pendingAckMessagesMap.remove(txnID);
        List<Position> positions = pendingAckMessage.values();
        // Materialize all single acks.
        cursor.asyncDelete(positions, deleteCallback, positions);
        positions.forEach(position -> this.pendingAckMessages.remove(position));

        // Materialize cumulative ack.
        cursor.asyncMarkDelete(this.pendingCumulativeAckMessage, (null == properties)?
                Collections.emptyMap() : properties, markDeleteCallback, this.pendingCumulativeAckMessage);

        // Reset txdID and position for cumulative ack.
        TXNID_UPDATER.set(this, null);
        POSITION_UPDATER.set(this, null);
    }

    /**
     * Abort a transaction.
     *
     * @param txnID  {@link TxnID} to identify the transaction.
     */
    public synchronized void abortTxn(TxnID txnID) {
        if (!this.pendingAckMessagesMap.containsKey(txnID)) {
            String errorMsg = String.format("[%s][%s] Transaction with id:%l%l not found.", topicName, subName,
                    txnID.getMostSigBits(), txnID.getLeastSigBits());
            log.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        ConcurrentOpenHashSet<Position> pendingAckMessage = this.pendingAckMessagesMap.remove(txnID);
        pendingAckMessage.forEach(position -> this.pendingAckMessages.remove(position));

        // Reset txdID and position for cumulative ack.
        TXNID_UPDATER.set(this, null);
        POSITION_UPDATER.set(this, null);
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentSubscription.class);
}
