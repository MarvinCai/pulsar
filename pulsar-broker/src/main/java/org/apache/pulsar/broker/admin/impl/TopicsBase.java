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
package org.apache.pulsar.broker.admin.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.rest.data.AckMessageRequest;
import org.apache.pulsar.broker.rest.data.AckMessageResponse;
import org.apache.pulsar.broker.rest.data.ConsumeMessageRequest;
import org.apache.pulsar.broker.rest.data.ConsumeMessageResponse;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.rest.RestMessagePublishContext;
import org.apache.pulsar.broker.rest.data.CreateConsumerRequest;
import org.apache.pulsar.broker.rest.data.CreateConsumerResponse;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.schema.SchemaRegistry;
import org.apache.pulsar.broker.service.schema.exceptions.SchemaException;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.websocket.data.ProducerAck;
import org.apache.pulsar.websocket.data.ProducerAcks;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.apache.pulsar.websocket.data.ProducerMessages;

/**
 *
 */
@Slf4j
public class TopicsBase extends PersistentTopicsBase {

    private static final Random random = new Random(System.currentTimeMillis());

    private static ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<Integer>> owningTopics =
            new ConcurrentOpenHashMap<>();

    private static ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, SchemaData>> subscriptions =
            new ConcurrentOpenHashMap<>();

    private static String defaultProducerName = "RestProducer";

    protected  void publishMessages(AsyncResponse asyncResponse, ProducerMessages request,
                                           boolean authoritative) {
        String topic = topicName.getPartitionedTopicName();
        if (owningTopics.containsKey(topic) || !findOwnerBrokerForTopic(authoritative, asyncResponse)) {
            // if we've done look up or or after look up this broker owns some of the partitions
            // then proceed to publish message else asyncResponse will be complete by look up.
            addOrGetSchemaForTopic(getSchemaData(request.getKeySchema(), request.getValueSchema()),
                new LongSchemaVersion(request.getSchemaVersion()))
            .thenAccept(schemaMeta -> {
                // Both schema version and schema data are necessary.
                if (schemaMeta.getLeft() != null && schemaMeta.getRight() != null) {
                    KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo =
                            KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaMeta.getLeft().toSchemaInfo());
                    publishMessagesToMultiplePartitions(topicName, request, owningTopics.get(topic), asyncResponse,
                            (KeyValueSchema) KeyValueSchema.of(AutoConsumeSchema.getSchema(kvSchemaInfo.getKey()),
                                    AutoConsumeSchema.getSchema(kvSchemaInfo.getValue())), schemaMeta.getRight());
                } else {
                    asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Fail to add or retrieve schema."));
                }
            }).exceptionally(e -> {
                if (log.isDebugEnabled()) {
                    log.warn("Fail to add or retrieve schema: " + e.getLocalizedMessage());
                }
                asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Fail to add or retrieve schema."));
                return null;
            });
        }
    }

    protected void publishMessagesToPartition(AsyncResponse asyncResponse, ProducerMessages request,
                                                     boolean authoritative, int partition) {
        if (topicName.isPartitioned()) {
            asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Topic name can't contain "
                    + "'-partition-' suffix."));
        }
        String topic = topicName.getPartitionedTopicName();
        // If broker owns the partition then proceed to publish message, else do look up.
        if ((owningTopics.containsKey(topic) && owningTopics.get(topic).contains(partition))
                || !findOwnerBrokerForTopic(authoritative, asyncResponse)) {
            addOrGetSchemaForTopic(getSchemaData(request.getKeySchema(), request.getValueSchema()),
                new LongSchemaVersion(request.getSchemaVersion()))
            .thenAccept(schemaMeta -> {
                // Both schema version and schema data are necessary.
                if (schemaMeta.getLeft() != null && schemaMeta.getRight() != null) {
                    KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo = KeyValueSchemaInfo
                            .decodeKeyValueSchemaInfo(schemaMeta.getLeft().toSchemaInfo());
                    publishMessagesToSinglePartition(topicName, request, partition, asyncResponse,
                            (KeyValueSchema) KeyValueSchema.of(AutoConsumeSchema.getSchema(kvSchemaInfo.getKey()),
                                    AutoConsumeSchema.getSchema(kvSchemaInfo.getValue())), schemaMeta.getRight());
                } else {
                    asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Fail to add or retrieve schema."));
                }
            }).exceptionally(e -> {
                if (log.isDebugEnabled()) {
                    log.warn("Fail to add or retrieve schema: " + e.getLocalizedMessage());
                }
                asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Fail to add or retrieve schema."));
                return null;
            });
        }
    }

    private CompletableFuture<PositionImpl> publishSingleMessageToPartition(String topic, Message message) {
        CompletableFuture<PositionImpl> publishResult = new CompletableFuture<>();
        pulsar().getBrokerService().getTopic(topic, false)
        .thenAccept(t -> {
            // TODO: Check message backlog
            if (!t.isPresent()) {
                // Topic not found, and remove from owning partition list.
                publishResult.completeExceptionally(new BrokerServiceException.TopicNotFoundException("Topic not "
                        + "owned by current broker."));
                TopicName topicName = TopicName.get(topic);
                owningTopics.get(topicName.getPartitionedTopicName()).remove(topicName.getPartitionIndex());
            } else {
                t.get().publishMessage(messageToByteBuf(message),
                RestMessagePublishContext.get(publishResult, t.get(), System.nanoTime()));
            }
        });

        return publishResult;
    }

    private void publishMessagesToSinglePartition(TopicName topicName, ProducerMessages request,
                                                  int partition, AsyncResponse asyncResponse,
                                                  KeyValueSchema keyValueSchema, SchemaVersion schemaVersion) {
        try {
            String producerName = (null == request.getProducerName() || request.getProducerName().isEmpty())
                    ? defaultProducerName : request.getProducerName();
            List<Message> messages = buildMessage(request, keyValueSchema, producerName);
            List<CompletableFuture<PositionImpl>> publishResults = new ArrayList<>();
            List<ProducerAck> produceMessageResults = new ArrayList<>();
            for (int index = 0; index < messages.size(); index++) {
                ProducerAck produceMessageResult = new ProducerAck();
                produceMessageResult.setMessageId(partition + "");
                produceMessageResults.add(produceMessageResult);
                publishResults.add(publishSingleMessageToPartition(topicName.getPartition(partition).toString(),
                        messages.get(index)));
            }
            FutureUtil.waitForAll(publishResults).thenRun(() -> {
                processPublishMessageResults(produceMessageResults, publishResults);
                asyncResponse.resume(Response.ok().entity(new ProducerAcks(produceMessageResults,
                        ((LongSchemaVersion) schemaVersion).getVersion())).build());
            }).exceptionally(e -> {
                processPublishMessageResults(produceMessageResults, publishResults);
                asyncResponse.resume(Response.ok().entity(new ProducerAcks(produceMessageResults,
                        ((LongSchemaVersion) schemaVersion).getVersion())).build());
                return null;
            });
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.warn("Fail publish message with rest produce message request for topic  {}: {} ",
                        topicName, e.getCause());
            }
            asyncResponse.resume(new RestException(Status.BAD_REQUEST, e.getMessage()));
        }
    }

    private void publishMessagesToMultiplePartitions(TopicName topicName, ProducerMessages request,
                                                     ConcurrentOpenHashSet<Integer> partitionIndexes,
                                                     AsyncResponse asyncResponse, KeyValueSchema keyValueSchema,
                                                     SchemaVersion schemaVersion) {
        try {
            String producerName = (null == request.getProducerName() || request.getProducerName().isEmpty())
                    ? defaultProducerName : request.getProducerName();
            List<Message> messages = buildMessage(request, keyValueSchema, producerName);
            List<CompletableFuture<PositionImpl>> publishResults = new ArrayList<>();
            List<ProducerAck> produceMessageResults = new ArrayList<>();
            List<Integer> owningPartitions = partitionIndexes.values();
            for (int index = 0; index < messages.size(); index++) {
                ProducerAck produceMessageResult = new ProducerAck();
                produceMessageResult.setMessageId(owningPartitions.get(index % (int) partitionIndexes.size()) + "");
                produceMessageResults.add(produceMessageResult);
                publishResults.add(publishSingleMessageToPartition(topicName.getPartition(owningPartitions
                                .get(index % (int) partitionIndexes.size())).toString(),
                    messages.get(index)));
            }
            FutureUtil.waitForAll(publishResults).thenRun(() -> {
                processPublishMessageResults(produceMessageResults, publishResults);
                asyncResponse.resume(Response.ok().entity(new ProducerAcks(produceMessageResults,
                        ((LongSchemaVersion) schemaVersion).getVersion())).build());
            }).exceptionally(e -> {
                processPublishMessageResults(produceMessageResults, publishResults);
                asyncResponse.resume(Response.ok().entity(new ProducerAcks(produceMessageResults,
                        ((LongSchemaVersion) schemaVersion).getVersion())).build());
                return null;
            });
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.warn("Fail publish message with rest produce message request for topic  {}: {} ",
                        topicName, e.getCause());
            }
            e.printStackTrace();
            asyncResponse.resume(new RestException(Status.BAD_REQUEST, e.getMessage()));
        }
    }

    private void processPublishMessageResults(List<ProducerAck> produceMessageResults,
                                              List<CompletableFuture<PositionImpl>> publishResults) {
        // process publish message result
        for (int index = 0; index < publishResults.size(); index++) {
            try {
                PositionImpl position = publishResults.get(index).get();
                MessageId messageId = new MessageIdImpl(position.getLedgerId(), position.getEntryId(),
                        Integer.parseInt(produceMessageResults.get(index).getMessageId()));
                produceMessageResults.get(index).setMessageId(messageId.toString());
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.warn("Fail publish [{}] message with rest produce message request for topic  {}: {} ",
                            index, topicName);
                }
                if (e instanceof BrokerServiceException.TopicNotFoundException) {
                    // Topic ownership might changed, force to look up again.
                    owningTopics.remove(topicName.getPartitionedTopicName());
                }
                extractException(e, produceMessageResults.get(index));
            }
        }
    }

    private void extractException(Exception e, ProducerAck produceMessageResult) {
        if (!(e instanceof BrokerServiceException.TopicFencedException && e instanceof ManagedLedgerException)) {
            produceMessageResult.setErrorCode(2);
        } else {
            produceMessageResult.setErrorCode(1);
        }
        produceMessageResult.setErrorMsg(e.getMessage());
    }

    // Look up topic owner for given topic.
    // Return if asyncResponse has been completed.
    private boolean findOwnerBrokerForTopic(boolean authoritative, AsyncResponse asyncResponse) {
        PartitionedTopicMetadata metadata = internalGetPartitionedMetadata(authoritative, false);
        List<String> redirectAddresses = Collections.synchronizedList(new ArrayList<>());
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        List<CompletableFuture<Void>> lookupFutures = new ArrayList<>();
        if (!topicName.isPartitioned() && metadata.partitions > 1) {
            // Partitioned topic with multiple partitions, need to do look up for each partition.
            for (int index = 0; index < metadata.partitions; index++) {
                lookupFutures.add(lookUpBrokerForTopic(topicName.getPartition(index),
                        authoritative, redirectAddresses));
            }
        } else {
            // Non-partitioned topic or specific topic partition.
            lookupFutures.add(lookUpBrokerForTopic(topicName, authoritative, redirectAddresses));
        }

        FutureUtil.waitForAll(lookupFutures)
        .thenRun(() -> {
            // Current broker doesn't own the topic or any partition of the topic, redirect client to a broker
            // that own partition of the topic or know who own partition of the topic.
            if (!owningTopics.containsKey(topicName.getPartitionedTopicName())) {
                if (redirectAddresses.isEmpty()) {
                    // No broker to redirect, means look up for some partitions failed,
                    // client should retry with other brokers.
                    asyncResponse.resume(new RestException(Status.NOT_FOUND, "Can't find owner of given topic."));
                    future.complete(true);
                } else {
                    // Redirect client to other broker owns the topic or know which broker own the topic.
                    try {
                        URI redirectURI = new URI(String.format("%s%s", redirectAddresses.get(0), uri.getPath()));
                        asyncResponse.resume(Response.temporaryRedirect(redirectURI));
                        future.complete(true);
                    } catch (URISyntaxException | NullPointerException e) {
                        log.error("Error in preparing redirect url with rest produce message request for topic  {}: {}",
                                topicName, e.getMessage(), e);
                        asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR,
                                "Fail to redirect client request."));
                        future.complete(true);
                    }
                }
            } else {
                future.complete(false);
            }
        }).exceptionally(e -> {
            if (log.isDebugEnabled()) {
                log.warn("Fail to look up topic: " + e.getCause());
            }
            asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR, "Error look up topic: "
                    + e.getLocalizedMessage()));
            future.complete(true);
            return null;
        });
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            if (log.isDebugEnabled()) {
                log.warn("Fail to lookup topic for rest produce message request for topic {}.", topicName.toString());
            }
            return true;
        }
    }

    // Look up topic owner for non-partitioned topic or single topic partition.
    private CompletableFuture<Void> lookUpBrokerForTopic(TopicName partitionedTopicName,
                                                         boolean authoritative, List<String> redirectAddresses) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture<Optional<LookupResult>> lookupFuture = pulsar().getNamespaceService()
                .getBrokerServiceUrlAsync(partitionedTopicName,
                        LookupOptions.builder().authoritative(authoritative).loadTopicsInBundle(false).build());

        lookupFuture.thenAccept(optionalResult -> {
            if (optionalResult == null || !optionalResult.isPresent()) {
                if (log.isDebugEnabled()) {
                    log.warn("Fail to lookup topic for rest produce message request for topic {}.",
                            partitionedTopicName);
                }
                completeLookup(Pair.of(Collections.emptyList(), false), redirectAddresses, future);
                return;
            }

            LookupResult result = optionalResult.get();
            if (result.getLookupData().getHttpUrl().equals(pulsar().getWebServiceAddress())) {
                pulsar().getBrokerService().getLookupRequestSemaphore().release();
                // Current broker owns the topic, add to owning topic.
                if (log.isDebugEnabled()) {
                    log.warn("Complete topic look up for rest produce message request for topic {}, "
                                    + "current broker is owner broker: {}",
                            partitionedTopicName, result.getLookupData());
                }
                owningTopics.computeIfAbsent(partitionedTopicName.getPartitionedTopicName(),
                        (key) -> new ConcurrentOpenHashSet<Integer>()).add(partitionedTopicName.getPartitionIndex());
                completeLookup(Pair.of(Collections.emptyList(), false), redirectAddresses, future);
            } else {
                // Current broker doesn't own the topic or doesn't know who own the topic.
                if (log.isDebugEnabled()) {
                    log.warn("Complete topic look up for rest produce message request for topic {}, "
                                    + "current broker is not owner broker: {}",
                            partitionedTopicName, result.getLookupData());
                }
                if (result.isRedirect()) {
                    // Redirect lookup.
                    completeLookup(Pair.of(Arrays.asList(result.getLookupData().getHttpUrl(),
                            result.getLookupData().getHttpUrlTls()), false), redirectAddresses, future);
                } else {
                    // Found owner for topic.
                    completeLookup(Pair.of(Arrays.asList(result.getLookupData().getHttpUrl(),
                            result.getLookupData().getHttpUrlTls()), true), redirectAddresses, future);
                }
            }
        }).exceptionally(exception -> {
            log.warn("Failed to lookup broker with rest produce message request for topic {}: {}",
                    partitionedTopicName, exception.getMessage(), exception);
            completeLookup(Pair.of(Collections.emptyList(), false), redirectAddresses, future);
            return null;
        });
        return future;
    }

    private CompletableFuture<Pair<SchemaData, SchemaVersion>> addOrGetSchemaForTopic(SchemaData schemaData,
                                                                                      SchemaVersion schemaVersion) {
        CompletableFuture<Pair<SchemaData, SchemaVersion>> future = new CompletableFuture<>();
        if (null != schemaData) {
            SchemaVersion sv;
            try {
                sv = addSchema(schemaData).get();
                future.complete(Pair.of(schemaData, sv));
            } catch (InterruptedException | ExecutionException e) {
                future.complete(Pair.of(null, null));
            }
        } else if (null != schemaVersion) {
            String id = TopicName.get(topicName.getPartitionedTopicName()).getSchemaName();
            SchemaRegistry.SchemaAndMetadata schemaAndMetadata;
            try {
                schemaAndMetadata = pulsar().getSchemaRegistryService().getSchema(id, schemaVersion).get();
                future.complete(Pair.of(schemaAndMetadata.schema, schemaAndMetadata.version));
            } catch (InterruptedException | ExecutionException e) {
                future.complete(Pair.of(null, null));
            }
        } else {
            future.complete(Pair.of(null, null));
        }
        return future;
    }

    private CompletableFuture<SchemaVersion> addSchema(SchemaData schemaData) {
        // Only need to add to first partition the broker owns since the schema id in schema registry are
        // same for all partitions which is the partitionedTopicName
        List<Integer> partitions = owningTopics.get(topicName.getPartitionedTopicName()).values();
        CompletableFuture<SchemaVersion> result = new CompletableFuture<>();
        for (int index = 0; index < partitions.size(); index++) {
            CompletableFuture<SchemaVersion> future = new CompletableFuture<>();
            String topicPartitionName = topicName.getPartition(partitions.get(index)).toString();
            pulsar().getBrokerService().getTopic(topicPartitionName, false)
            .thenAccept(topic -> {
                if (!topic.isPresent()) {
                    future.completeExceptionally(new BrokerServiceException.TopicNotFoundException(
                            "Topic " + topicPartitionName + " not found"));
                } else {
                    topic.get().addSchema(schemaData).thenAccept(schemaVersion -> future.complete(schemaVersion))
                    .exceptionally(exception -> {
                        exception.printStackTrace();
                        future.completeExceptionally(exception);
                        return null;
                    });
                }
            });
            try {
                result.complete(future.get());
                break;
            } catch (Exception e) {
                result.completeExceptionally(new SchemaException("Unable to add schema " + schemaData
                        + " to topic " + topicName.getPartitionedTopicName()));
            }
        }
        return result;
    }

    private SchemaData getSchemaData(String keySchema, String valueSchema) {
        try {
            if ((keySchema == null || keySchema.isEmpty()) && (valueSchema == null || valueSchema.isEmpty())) {
                return null;
            } else {
                SchemaInfo keySchemaInfo = (keySchema == null || keySchema.isEmpty())
                        ? StringSchema.utf8().getSchemaInfo() : ObjectMapperFactory.getThreadLocal()
                        .readValue(Base64.getDecoder().decode(keySchema), SchemaInfo.class);
                SchemaInfo valueSchemaInfo = (valueSchema == null || valueSchema.isEmpty())
                        ? StringSchema.utf8().getSchemaInfo() : ObjectMapperFactory.getThreadLocal()
                        .readValue(Base64.getDecoder().decode(valueSchema), SchemaInfo.class);
                if (null == keySchemaInfo.getName()) {
                    keySchemaInfo.setName(keySchemaInfo.getType().toString());
                }
                if (null == valueSchemaInfo.getName()) {
                    valueSchemaInfo.setName(valueSchemaInfo.getType().toString());
                }
                SchemaInfo schemaInfo = KeyValueSchemaInfo.encodeKeyValueSchemaInfo("KVSchema-"
                                + topicName.getPartitionedTopicName(),
                        keySchemaInfo, valueSchemaInfo,
                        KeyValueEncodingType.SEPARATED);
                return SchemaData.builder()
                        .data(schemaInfo.getSchema())
                        .isDeleted(false)
                        .user("Rest Producer")
                        .timestamp(System.currentTimeMillis())
                        .type(schemaInfo.getType())
                        .props(schemaInfo.getProperties())
                        .build();
            }
        } catch (IOException e) {
            if (log.isDebugEnabled()) {
                log.warn("Fail to parse schema info for rest produce request with key schema {} and value schema {}"
                        , keySchema, valueSchema);
            }
            return null;
        }
    }

    // Convert message to ByteBuf
    public ByteBuf messageToByteBuf(Message message) {
        checkArgument(message instanceof MessageImpl, "Message must be type of MessageImpl.");

        MessageImpl msg = (MessageImpl) message;
        MessageMetadata messageMetadata = msg.getMessageBuilder();
        ByteBuf payload = msg.getDataBuffer();
        messageMetadata.setCompression(CompressionCodecProvider.convertToWireProtocol(CompressionType.NONE));
        messageMetadata.setUncompressedSize(payload.readableBytes());

        ByteBuf byteBuf = null;
        try {
            byteBuf = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata, payload);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return byteBuf;
    }

    // Build pulsar message from serialized message.
    private List<Message> buildMessage(ProducerMessages producerMessages, KeyValueSchema keyValueSchema,
                                       String producerName) {
        List<ProducerMessage> messages;
        List<Message> pulsarMessages = new ArrayList<>();

        messages = producerMessages.getMessages();
        for (ProducerMessage message : messages) {
            try {
                MessageMetadata messageMetadata = new MessageMetadata();
                messageMetadata.setProducerName(producerName);
                messageMetadata.setPublishTime(System.currentTimeMillis());
                messageMetadata.setSequenceId(message.getSequenceId());
                if (null != message.getReplicationClusters()) {
                    messageMetadata.addAllReplicateTos(message.getReplicationClusters());
                }

                if (null != message.getProperties()) {
                    messageMetadata.addAllProperties(message.getProperties().entrySet().stream().map(entry -> {
                        org.apache.pulsar.common.api.proto.KeyValue keyValue =
                                new org.apache.pulsar.common.api.proto.KeyValue();
                        keyValue.setKey(entry.getKey());
                        keyValue.setValue(entry.getValue());
                        return keyValue;
                    }).collect(Collectors.toList()));
                }
                if (null != message.getKey()) {
                    if (keyValueSchema.getKeySchema().getSchemaInfo().getType() == SchemaType.JSON) {
                        messageMetadata.setPartitionKey(new String(processJSONMsg(message.getKey())));
                    } else {
                        messageMetadata.setPartitionKey(new String(keyValueSchema.getKeySchema()
                                .encode(message.getKey())));
                    }
                }
                if (null != message.getEventTime() && !message.getEventTime().isEmpty()) {
                    messageMetadata.setEventTime(Long.valueOf(message.getEventTime()));
                }
                if (message.isDisableReplication()) {
                    messageMetadata.clearReplicateTo();
                    messageMetadata.addReplicateTo("__local__");
                }
                if (message.getDeliverAt() != 0 && messageMetadata.hasEventTime()) {
                    messageMetadata.setDeliverAtTime(message.getDeliverAt());
                } else if (message.getDeliverAfterMs() != 0) {
                    messageMetadata.setDeliverAtTime(messageMetadata.getEventTime() + message.getDeliverAfterMs());
                }
                if (keyValueSchema.getValueSchema().getSchemaInfo().getType() == SchemaType.JSON) {
                    pulsarMessages.add(MessageImpl.create(messageMetadata,
                            ByteBuffer.wrap(processJSONMsg(message.getPayload())), keyValueSchema.getValueSchema()));
                } else {
                    pulsarMessages.add(MessageImpl.create(messageMetadata,
                            ByteBuffer.wrap(keyValueSchema.getValueSchema().encode(message.getPayload())),
                            keyValueSchema.getValueSchema()));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return pulsarMessages;
    }

    private byte[] processJSONMsg(String msg) throws JsonProcessingException {
        return ObjectMapperFactory.getThreadLocal()
                .writeValueAsBytes(ObjectMapperFactory.getThreadLocal().readTree(msg));
    }

    private synchronized void completeLookup(Pair<List<String>, Boolean> result, List<String> redirectAddresses,
                                              CompletableFuture<Void> future) {
        pulsar().getBrokerService().getLookupRequestSemaphore().release();
        if (!result.getLeft().isEmpty()) {
            if (result.getRight()) {
                // If address is for owner of topic partition, add to head and it'll have higher priority
                // compare to broker for look redirect.
                redirectAddresses.add(0, isRequestHttps() ? result.getLeft().get(1) : result.getLeft().get(0));
            } else {
                redirectAddresses.add(redirectAddresses.size(), isRequestHttps()
                        ? result.getLeft().get(1) : result.getLeft().get(0));
            }
        }
        future.complete(null);
    }





    // Consume messages
    protected void createConsumer(AsyncResponse asyncResponse, CreateConsumerRequest request,
                                  boolean authoritative, String subscriptionName) {
        String topic = topicName.getPartitionedTopicName();
        if (owningTopics.containsKey(topic) || !findOwnerBrokerForTopic(authoritative, asyncResponse)) {
            internalCreateConsumer(asyncResponse, request, subscriptionName);
        }
    }

    // For rest api, adding a consumer is only adding the consumer name to subscriptions map to keep track of
    // consumers according to the subscription type, subscription keep track of consuming position with cursor
    // there's no dispatch related logic here.
    private void internalCreateConsumer(AsyncResponse asyncResponse, CreateConsumerRequest request,  String subscription) {
        buildOrGetSchemaData(request.getKeySchema(), request.getValueSchema(), request.getSchemaVersion())
                .thenAccept(schemaData -> {
                    String consumerId = String.format("%s_%s_%s", subscription, request.getConsumerName(), UUID.randomUUID());
                    // Generate a key from partition topic name and subscription name to keep track of all rest consumer for this topic.
                    String subKey = String.format("%s_%s", topicName, subscription);
                    log.info("*************");
                    log.info(pulsar().getWebServiceAddress());
                    log.info(pulsar().getWebServiceAddressTls());
                    subscriptions.computeIfAbsent(subKey, k-> new ConcurrentOpenHashMap<>()).put(consumerId, schemaData);
                    subscriptions.forEach((k, v) -> log.info("kk" + k + "----" + v));
                    asyncResponse.resume(Response.ok().entity(new CreateConsumerResponse(consumerId, isRequestHttps()? pulsar().getWebServiceAddressTls() : pulsar().getWebServiceAddress())).build());
                }).exceptionally(e -> {
            asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage()));
            return null;
        });
    }

    private CompletableFuture<SchemaData> buildOrGetSchemaData(String keySchema, String valueSchema, long schemaVersion) {
        CompletableFuture<SchemaData> future = new CompletableFuture<>();
        SchemaData schemaData = getSchemaData(keySchema, valueSchema);
        if (null != schemaData) {
            future.complete(schemaData);
        } else {
            if (schemaVersion < 0) {
                future.completeExceptionally(new IllegalArgumentException("Must provided either key/value schema or desired schema version to consume message."));
            } else {
                String id = TopicName.get(topicName.getPartitionedTopicName()).getSchemaName();
                pulsar().getSchemaRegistryService().getSchema(id, new LongSchemaVersion(schemaVersion))
                        .thenAccept(schemaAndMetadata -> future.complete(schemaAndMetadata.schema));
            }
        }
        return future;
    }

    protected void consumeMessages(AsyncResponse asyncResponse, ConsumeMessageRequest request, String subscription, String consumerId) {
        String topic = topicName.getPartitionedTopicName();
        String subKey = String.format("%s_%s", topicName, subscription);
        log.info("*************");
        log.info(subKey);
        subscriptions.forEach((k, v) -> log.info("kk" + k + "----" + v));
        if (owningTopics.containsKey(topic) && subscriptions.containsKey(subKey) && subscriptions.get(subKey).containsKey(consumerId)) {
            internalConsumeMessages(asyncResponse, request, subscription, consumerId);
        } else {
            asyncResponse.resume(new RestException(Status.BAD_REQUEST, "ConsumerId not found or broker doesn't own the topic"));
        }
    }

    private void internalConsumeMessages(AsyncResponse asyncResponse, ConsumeMessageRequest request, String subscription, String consumerId) {
        //If non partitioned topic or single partition of partitioned topic, read message from single partition
        //If multiple partition topic, start reading from a random partition this broker own.
        List<ConsumeMessageResponse.ConsumeMessageResult> consumeMessageResults = new ArrayList<>();
        long startEpoch = System.currentTimeMillis();
        String subKey = topicName + "_" + subscription;
        AtomicInteger messagesRead = new AtomicInteger(0);
        AtomicInteger byteRead = new AtomicInteger(0);
        List<Integer> owningPartitions = owningTopics.get(topicName.getPartitionedTopicName()).values();
        // Start to read from random partition this broker owns so message can be read evenly from all partitions
        // And if we can't read enough message from all partitoins in first round then from second round will just start from first partition.
        int startingIndex = random.nextInt(owningPartitions.size());
        KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo = KeyValueSchemaInfo.decodeKeyValueSchemaInfo(subscriptions.get(subKey).get(consumerId).toSchemaInfo());
        KeyValueSchema schema = (KeyValueSchema) KeyValueSchema.of(AutoConsumeSchema.getSchema(kvSchemaInfo.getKey()),
                AutoConsumeSchema.getSchema(kvSchemaInfo.getValue()));
        while (System.currentTimeMillis() - startEpoch < request.getTimeout() && messagesRead.get() < request.getMaxMessage()
                && byteRead.get() < request.getMaxByte()) {
            int numOfEntryToRead = request.getMaxMessage() - messagesRead.get();
            int partitionsToRead = owningPartitions.size() - startingIndex % owningPartitions.size();
            List<CompletableFuture<Pair<List<Entry>, Subscription>>> futures = new ArrayList<>();
            for (int index = startingIndex % owningPartitions.size(); index < owningPartitions.size(); index++, startingIndex++) {
                futures.add(internalConsumeMessageFromSinglePartition(topicName.getPartition(owningPartitions.get(index)).toString(), subscription,
                        numOfEntryToRead / partitionsToRead + 1));
            }
            int curIndex = startingIndex;
            FutureUtil.waitForAll(futures).thenRun(() -> {
                processReadMessageResults(messagesRead, byteRead, request, futures, owningPartitions,schema,
                        curIndex, subscription, consumeMessageResults);
            }).exceptionally(e -> {
                //if read message request failed on any partition that future will be 'skipped'
                if (log.isDebugEnabled()) {
                    log.warn("Fail to read message for rest consumer request on subscription: {} for topic {}: {}",
                            subscription, topicName, e);
                }
                processReadMessageResults(messagesRead, byteRead, request, futures, owningPartitions,schema,
                        curIndex, subscription, consumeMessageResults);
                return null;
            });
        }
        asyncResponse.resume(new ConsumeMessageResponse(consumeMessageResults));
    }

    private CompletableFuture<Pair<List<Entry>, Subscription>> internalConsumeMessageFromSinglePartition(String topicName, String subscriptionName, int numberOfEntryToRead) {
        CompletableFuture<Pair<List<Entry>, Subscription>> readFuture = new CompletableFuture<>();
        pulsar().getBrokerService().getTopic(topicName, false).thenAccept(topic -> {
            if (!topic.isPresent()) {
                // process error, topic not owned by the broker anymore
                if (log.isDebugEnabled()) {
                    log.warn("Trying to rest consume message on subscription: {} for topic {} but topic is not found",
                            subscriptionName, topicName);
                }
                readFuture.complete(Pair.of(ImmutableList.of(), null));
            } else {
                Subscription subscription = topic.get().getSubscription(subscriptionName);
                if (null == subscription) {
                    if (log.isDebugEnabled()) {
                        log.warn("Trying to rest consume message on subscription: {} for topic {} but subscription is not found",
                                subscriptionName, topicName);
                    }
                    readFuture.complete(Pair.of(ImmutableList.of(), null));
                } else {
                    if (subscription instanceof PersistentSubscription) {
                        // synchronize on the subscription so concurrent read won't mess up cursor's readPosition.
                        synchronized (subscription) {
                            // Persistent subscription.
                            ((PersistentSubscription) subscription).getCursor().asyncReadEntries(numberOfEntryToRead,
                                new AsyncCallbacks.ReadEntriesCallback() {
                                    @Override
                                    public void readEntriesComplete(List<Entry> entries, Object ctx) {
                                        readFuture.complete(Pair.of(entries, (Subscription)ctx));
                                    }

                                    @Override
                                    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                                        readFuture.complete(Pair.of(ImmutableList.of(), null));
                                    }
                                }, subscription, ((PersistentTopic) topic.get()).getMaxReadPosition());
                        }
                    } else {
                        // Non-persistent subscription can't be supported yet as message published to non-persistent topic need to
                        // be immediately deliver to consumer connected, and Rest consumers can't do that.
                        readFuture.complete(Pair.of(ImmutableList.of(), null));
                    }
                }
            }
        });
        return readFuture;
    }

    private void processReadMessageResults(AtomicInteger messagesRead, AtomicInteger byteRead, ConsumeMessageRequest request,
                                           List<CompletableFuture<Pair<List<Entry>, Subscription>>> futures, List<Integer> owningPartitions,
                                           KeyValueSchema schema, int curStartIndex, String subName,
                                           List<ConsumeMessageResponse.ConsumeMessageResult> messages ) {
        for (int index = 0; index < futures.size(); index++) {
            try {
                Pair<List<Entry>, Subscription> entriesAndCtx = futures.get(index).get();
                List<Entry> entries = entriesAndCtx.getLeft();
                if (messagesRead.get() > request.getMaxMessage()
                        || byteRead.get() > request.getMaxByte()) {
                    if (null != entriesAndCtx.getRight() && 0 != entries.size()) {
                        // reset read position
                        ((PersistentSubscription) entriesAndCtx.getLeft()).resetCursor(PositionImpl.get(
                                entries.get(0).getLedgerId(), entries.get(0).getEntryId()
                        ));
                    }
                } else {
                    for (int i = 0; i < entries.size(); i++) {
                        Entry entry = entries.get(i);
                        ByteBuf metadataAndPayload = entry.getDataBuffer();
                        MessageMetadata messageMetadata = Commands.parseMessageMetadata(metadataAndPayload);
                        int messageSize = metadataAndPayload.readableBytes();
                        if (messagesRead.get() + 1 > request.getMaxMessage()
                                || byteRead.get() + messageSize > request.getMaxByte()) {
                            // reset read position
                            log.info("&&&&&&&&&&&&&&&&");
                            log.info(entry.getLedgerId() + "%%%%%%%" + entry.getEntryId());
                            ((PersistentSubscription) entriesAndCtx.getRight()).getCursor().resetCursor(PositionImpl.get(
                                    entry.getLedgerId(), entry.getEntryId()
                            ));
                            break;
                        } else {
                            ConsumeMessageResponse.ConsumeMessageResult consumeMessagesResult = new ConsumeMessageResponse.ConsumeMessageResult();
                            consumeMessagesResult.setEventTime(messageMetadata.getEventTime());
                            consumeMessagesResult.setLedgerId(entry.getLedgerId());
                            consumeMessagesResult.setEntryId(entry.getEntryId());
                            consumeMessagesResult.setPartition(owningPartitions.get(curStartIndex % owningPartitions.size() + index));
                            consumeMessagesResult.setProperties(messageMetadata.getPropertiesList().toString());
                            log.info("$$$$$$$$$$$$$");
                            log.info(messageMetadata.getPartitionKey());
                            log.info(entry.getData().toString());
                            consumeMessagesResult.setKey(schema.getKeySchema().decode(Base64.getDecoder().decode(messageMetadata.getPartitionKey().getBytes())).toString());
                            consumeMessagesResult.setValue(schema.getValueSchema().decode(Base64.getDecoder().decode(entry.getData())).toString());
                            consumeMessagesResult.setSequenceId(messageMetadata.getSequenceId());
                            messages.add(consumeMessagesResult);
                            messagesRead.incrementAndGet();
                            byteRead.addAndGet(messageSize);
                        }
                    }
                }
            } catch (Exception e) {
                // If read message fail for some partitions, exception will be swallow here and we'll try to do
                // more read from other available partitions to fulfil the request.
                if (log.isDebugEnabled()) {
                    log.warn("Fail to read message for rest consumer request on subscription: {} for topic {}: {}",
                            subName, topicName, e);
                }
            }
        }
    }

    protected void ackMessage(AsyncResponse asyncResponse, AckMessageRequest request, String subscriptionName, String consumerId) {
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        List<Boolean> results = new ArrayList<>();
        for (int index = 0; index < request.getMessagePositions().size(); index++) {
            List<String> messagePosition = request.getMessagePositions().get(index);
            futures.add(internalAckMessage(request.getAckType(), Integer.valueOf(messagePosition.get(0)),
                    Long.valueOf(messagePosition.get(1)), Long.valueOf(messagePosition.get(2)), subscriptionName, consumerId));
        }
        FutureUtil.waitForAll(futures).thenRun(() -> {
            for (int index = 0; index < futures.size(); index++) {
                try {
                    results.add(futures.get(index).get());
                } catch (InterruptedException | ExecutionException e) {
                    if (log.isDebugEnabled()) {
                        log.warn("Try to rest ack message on subscription {} for topic {} with consumerId {}" +
                                        " on position {}:{} but ack failed", subscriptionName, topicName, consumerId,
                                request.getMessagePositions().get(index).get(1), request.getMessagePositions().get(index).get(2));
                    }
                    results.add(false);
                }
            }
            asyncResponse.resume(new AckMessageResponse(results));
        });

    }

    private CompletableFuture<Boolean> internalAckMessage(String ackType, int partition, long ledgerId, long entryId,
                                                          String subscriptionName, String consumerId) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        if (!owningTopics.get(topicName.getPartitionedTopicName()).contains(partition)) {
            if (log.isDebugEnabled()) {
                log.warn("Try to rest ack message on subscription {} for topic {} with consumerId {}" +
                        " but topic is not own by this broker", subscriptionName, topicName, consumerId);
            }
            future.complete(false);
            return future;
        }
        pulsar().getBrokerService().getTopic(topicName.getPartition(partition).toString(), false).
                thenAccept(topic -> {
                    if (!topic.isPresent()) {
                        if (log.isDebugEnabled()) {
                            log.warn("Try to rest ack message on subscription {} for topic {} with consumerId {}" +
                                    " but topic doesn't exist on this broker", subscriptionName, topicName, consumerId);
                        }
                        future.complete(false);
                    } else {
                        Subscription subscription = topic.get().getSubscription(subscriptionName);
                        if (null == subscription) {
                            if (log.isDebugEnabled()) {
                                log.warn("Try to rest ack message on subscription {} for topic {} with consumerId {}" +
                                        " but subscription doesn't exist on this broker", subscriptionName, topicName, consumerId);
                            }
                            future.complete(false);
                        }
                        switch (AckType.fromString(ackType)) {
                            case SINGLE:
                                ((PersistentSubscription) subscription).getCursor().asyncDelete(PositionImpl.get(ledgerId, entryId),
                                        new AsyncCallbacks.DeleteCallback() {
                                            @Override
                                            public void deleteComplete(Object ctx) {
                                                future.complete(true);
                                            }

                                            @Override
                                            public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                                                future.complete(false);
                                            }
                                        }, null);
                                break;
                            case CUMULATIVE:
                                ((PersistentSubscription) subscription).getCursor().asyncMarkDelete(PositionImpl.get(ledgerId, entryId),
                                        new AsyncCallbacks.MarkDeleteCallback() {
                                            @Override
                                            public void markDeleteComplete(Object ctx) {
                                                future.complete(true);
                                            }

                                            @Override
                                            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                                                future.complete(false);
                                            }
                                        }, null);

                                break;
                            default:
                                if (log.isDebugEnabled()) {
                                    log.warn("Try to rest ack message on subscription {} for topic {} with consumerId {}" +
                                            " but ack type {} is not recognized", subscriptionName, topicName, consumerId, ackType);
                                }
                                future.complete(false);
                        }
                    }
                }).exceptionally(e -> {
            if (log.isDebugEnabled()) {
                log.warn("Fail to rest ack message on subscription {} for topic {} with consumerId {}, {}",
                        subscriptionName, topicName, consumerId, e.getMessage());
            }
            future.complete(false);
            return null;
        });
        return future;
    }

    private enum AckType {
        SINGLE("single"),
        CUMULATIVE("cumulative");

        String type;

        AckType(String type) {
            this.type = type;
        }

        public static AckType fromString(String type) {
            for (AckType ackType : values()) {
                if (ackType.type.equalsIgnoreCase(type)) {
                    return ackType;
                }
            }
            return null;
        }
    }

}
