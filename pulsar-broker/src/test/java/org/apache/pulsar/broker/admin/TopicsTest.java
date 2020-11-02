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
package org.apache.pulsar.broker.admin;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Sets;
import org.apache.pulsar.broker.admin.v2.PersistentTopics;
import org.apache.pulsar.broker.admin.v3.Topics;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.policies.data.*;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ProducerAcks;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.apache.pulsar.websocket.data.ProducerMessages;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.timeout;

@PrepareForTest(PersistentTopics.class)
public class TopicsTest extends MockedPulsarServiceBaseTest {

    private Topics topics;
    private final String testLocalCluster = "test";
    private final String testTenant = "my-tenant";
    private final String testNamespace = "my-namespace";
    private final String testTopicName = "my-topic";

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        super.internalSetup();
        topics = spy(new Topics());
        topics.setPulsar(pulsar);
        doReturn(TopicDomain.persistent.value()).when(topics).domain();
        doReturn("test-app").when(topics).clientAppId();
        doReturn(mock(AuthenticationDataHttps.class)).when(topics).clientAuthData();
        admin.clusters().createCluster(testLocalCluster, new ClusterData("http://broker-use.com:8080"));
        admin.tenants().createTenant(testTenant,
                new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet(testLocalCluster)));
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace, Sets.newHashSet(testLocalCluster));
    }

    @Override
    @AfterMethod
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testProduceToNonPartitionedTopic() throws Exception {
        admin.topics().createNonPartitionedTopic("persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        Schema<String> schema = StringSchema.utf8();
        String key = "my-key", value = "my-value";
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(Base64.getEncoder().encodeToString(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()).getBytes()));
        producerMessages.setValueSchema(Base64.getEncoder().encodeToString(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()).getBytes()));
        String message = "[" +
                "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":1}," +
                "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":2}," +
                "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":3}]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message, new TypeReference<List<ProducerMessage>>() {}));
        topics.produceOnTopic(asyncResponse, testTenant, testNamespace, testTopicName, false, producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
        Object responseEntity = responseCaptor.getValue().getEntity();
        Assert.assertTrue(responseEntity instanceof ProducerAcks);
        ProducerAcks response = (ProducerAcks) responseEntity;
        Assert.assertEquals(response.getMessagePublishResults().size(), 3);
        Assert.assertEquals(response.getSchemaVersion(), 0);
        for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
            Assert.assertEquals(Integer.parseInt(response.getMessagePublishResults().get(index).getMessageId().split(":")[2]), -1);
            Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorCode(), 0);
            Assert.assertTrue(response.getMessagePublishResults().get(index).getMessageId().length() > 0);
        }
    }

    @Test
    public void testProduceToPartitionedTopic() throws Exception {
        admin.topics().createPartitionedTopic("persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName + "-p", 5);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        Schema<String> schema = StringSchema.utf8();
        String key = "my-key", value = "my-value";
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(Base64.getEncoder().encodeToString(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()).getBytes()));
        producerMessages.setValueSchema(Base64.getEncoder().encodeToString(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()).getBytes()));
        String message = "[" +
                "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":1}," +
                "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":2}," +
                "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":3}," +
                "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":4}," +
                "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":5}," +
                "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":6}," +
                "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":7}," +
                "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":8}," +
                "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":9}," +
                "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":10}]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message, new TypeReference<List<ProducerMessage>>() {}));
        topics.produceOnTopic(asyncResponse, testTenant, testNamespace, testTopicName + "-p", false, producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
        Object responseEntity = responseCaptor.getValue().getEntity();
        Assert.assertTrue(responseEntity instanceof ProducerAcks);
        ProducerAcks response = (ProducerAcks) responseEntity;
        Assert.assertEquals(response.getMessagePublishResults().size(), 10);
        Assert.assertEquals(response.getSchemaVersion(), 0);
        int[] messagePerPartition = new int[5];
        for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
            messagePerPartition[Integer.parseInt(response.getMessagePublishResults().get(index).getMessageId().split(":")[2])]++;
            Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorCode(), 0);
            Assert.assertTrue(response.getMessagePublishResults().get(index).getMessageId().length() > 0);
        }
        for (int index = 0; index < messagePerPartition.length; index++) {
            // We publish to each partition in round robin mode so each partition should get at most 2 message.
            Assert.assertTrue(messagePerPartition[index] <= 2);
        }
    }

    @Test
    public void testProduceToPartitionedTopicSpecificPartition() throws Exception {
        admin.topics().createPartitionedTopic("persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName, 5);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        Schema<String> schema = StringSchema.utf8();
        String key = "my-key", value = "my-value";
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(Base64.getEncoder().encodeToString(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()).getBytes()));
        producerMessages.setValueSchema(Base64.getEncoder().encodeToString(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()).getBytes()));
        String message = "[" +
                "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":1}," +
                "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":2}," +
                "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":3}," +
                "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":4}]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message, new TypeReference<List<ProducerMessage>>() {}));
        topics.produceOnTopicPartition(asyncResponse, testTenant, testNamespace, testTopicName, 2,false, producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
        Object responseEntity = responseCaptor.getValue().getEntity();
        Assert.assertTrue(responseEntity instanceof ProducerAcks);
        ProducerAcks response = (ProducerAcks) responseEntity;
        Assert.assertEquals(response.getMessagePublishResults().size(), 4);
        Assert.assertEquals(response.getSchemaVersion(), 0);
        for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
            Assert.assertEquals(Integer.parseInt(response.getMessagePublishResults().get(index).getMessageId().split(":")[2]), 2);
            Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorCode(), 0);
            Assert.assertTrue(response.getMessagePublishResults().get(index).getMessageId().length() > 0);
        }
    }

    @Test
    public void testProduceFailed() throws Exception {
        admin.topics().createNonPartitionedTopic("persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName);
        pulsar.getBrokerService().getTopic("persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName, false).thenAccept(topic -> {
            try {
                PersistentTopic mockPersistentTopic = spy((PersistentTopic) topic.get());
                AtomicInteger count = new AtomicInteger();
                doAnswer(new Answer() {
                    @Override
                    public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                        Topic.PublishContext publishContext = invocationOnMock.getArgument(1);
                        if (count.getAndIncrement() < 2) {
                            publishContext.completed(null, -1, -1);
                        } else {
                            publishContext.completed(new BrokerServiceException.TopicFencedException("Fake exception"), -1, -1);
                        }
                        return null;
                    }
                }).when(mockPersistentTopic).publishMessage(any(), any());
                BrokerService mockBrokerService = spy(pulsar.getBrokerService());
                doReturn(CompletableFuture.completedFuture(Optional.of(mockPersistentTopic))).when(mockBrokerService).getTopic(anyString(), anyBoolean());
                doReturn(mockBrokerService).when(pulsar).getBrokerService();
                AsyncResponse asyncResponse = mock(AsyncResponse.class);
                Schema<String> schema = StringSchema.utf8();
                String key = "my-key", value = "my-value";
                ProducerMessages producerMessages = new ProducerMessages();
                producerMessages.setKeySchema(Base64.getEncoder().encodeToString(ObjectMapperFactory.getThreadLocal().
                        writeValueAsString(schema.getSchemaInfo()).getBytes()));
                producerMessages.setValueSchema(Base64.getEncoder().encodeToString(ObjectMapperFactory.getThreadLocal().
                        writeValueAsString(schema.getSchemaInfo()).getBytes()));
                String message = "[" +
                        "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":1}," +
                        "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":2}," +
                        "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":3}," +
                        "{\"key\":\"enhjLWtleQ==\",\"payload\":\"enhjLXZhbHVl\",\"eventTime\":1603045262772,\"sequenceId\":4}]";
                producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message, new TypeReference<List<ProducerMessage>>() {}));
                // Previous request should trigger namespace bundle loading, retry produce.
                topics.produceOnTopic(asyncResponse, testTenant, testNamespace, testTopicName, false, producerMessages);
                ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
                verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
                Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
                Object responseEntity = responseCaptor.getValue().getEntity();
                Assert.assertTrue(responseEntity instanceof ProducerAcks);
                ProducerAcks response = (ProducerAcks) responseEntity;
                Assert.assertEquals(response.getMessagePublishResults().size(), 4);
                int errorResponse = 0;
                for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
                    int errorCode = response.getMessagePublishResults().get(index).getErrorCode();
                    if (0 == errorCode) {
                        Assert.assertEquals(Integer.parseInt(response.getMessagePublishResults().get(index).getMessageId().split(":")[2]), -1);
                        Assert.assertTrue(response.getMessagePublishResults().get(index).getMessageId().length() > 0);
                    } else {
                        errorResponse++;
                        Assert.assertEquals(errorCode, 2);
                        Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorMsg(),"org.apache.pulsar.broker.service.BrokerServiceException$TopicFencedException: Fake exception");
                    }
                }
                // Add entry start to fail after 2nd operation, we published 4 msg so expecting 2 error response.
                Assert.assertTrue(errorResponse == 2);
            } catch (Throwable e) {
                Assert.fail(e.getMessage());
            }
        }).get();
    }
}