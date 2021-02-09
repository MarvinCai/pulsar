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
package org.apache.pulsar.broker.admin.v3;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import org.apache.pulsar.broker.admin.impl.TopicsBase;
import org.apache.pulsar.broker.rest.data.ConsumeMessageRequest;
import org.apache.pulsar.broker.rest.data.CreateConsumerRequest;
import org.apache.pulsar.websocket.data.ProducerMessages;

@Path("/persistent/topics")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/", description = "Apis for produce,consume and ack message on topics.", tags = "topics")
public class Topics extends TopicsBase {

    @POST
    @Path("/{tenant}/{namespace}/{topic}")
    @ApiOperation(value = "Produce message to a topic.", response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error") })
    public void produceOnTopic(@Suspended final AsyncResponse asyncResponse,
                               @ApiParam(value = "Specify the tenant", required = true)
                               @PathParam("tenant") String tenant,
                               @ApiParam(value = "Specify the namespace", required = true)
                               @PathParam("namespace") String namespace,
                               @ApiParam(value = "Specify topic name", required = true)
                               @PathParam("topic") @Encoded String encodedTopic,
                               @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                               ProducerMessages producerMessages) {
        validateTopicName(tenant, namespace, encodedTopic);
        publishMessages(asyncResponse, producerMessages, authoritative);
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/partitions/{partition}")
    @ApiOperation(value = "Produce message to a topic.", response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error") })
    public void produceOnTopicPartition(@Suspended final AsyncResponse asyncResponse,
                                        @ApiParam(value = "Specify the tenant", required = true)
                                        @PathParam("tenant") String tenant,
                                        @ApiParam(value = "Specify the namespace", required = true)
                                        @PathParam("namespace") String namespace,
                                        @ApiParam(value = "Specify topic name", required = true)
                                        @PathParam("topic") @Encoded String encodedTopic,
                                        @ApiParam(value = "Specify topic partition", required = true)
                                        @PathParam("partition") int partition,
                                        @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                                        ProducerMessages producerMessages) {
        validateTopicName(tenant, namespace, encodedTopic);
        publishMessagesToPartition(asyncResponse, producerMessages, authoritative, partition);
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subscription}")
    @ApiOperation(value = "Create a consumer on a topic for subscription.", response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error") })
    public void createConsumer(@Suspended final AsyncResponse asyncResponse,
                               @ApiParam(value = "Specify the tenant", required = true)
                               @PathParam("tenant") String tenant,
                               @ApiParam(value = "Specify the namespace", required = true)
                               @PathParam("namespace") String namespace,
                               @ApiParam(value = "Specify topic name", required = true)
                               @PathParam("topic") @Encoded String encodedTopic,
                               @ApiParam(value = "Specify subscription name", required = true)
                               @PathParam("subscription") String subscription,
                               @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                               CreateConsumerRequest createConsumerRequest) {
        validateTopicName(tenant, namespace, encodedTopic);
        createConsumer(asyncResponse, createConsumerRequest, authoritative, subscription);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subscription}/consumer/{consumerId}/messages")
    @ApiOperation(value = "Consume messages on topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error") })
    public void consumeMessage(@Suspended final AsyncResponse asyncResponse,
                               @ApiParam(value = "Specify the tenant", required = true)
                               @PathParam("tenant") String tenant,
                               @ApiParam(value = "Specify the namespace", required = true)
                               @PathParam("namespace") String namespace,
                               @ApiParam(value = "Specify topic name", required = true)
                               @PathParam("topic") @Encoded String encodedTopic,
                               @ApiParam(value = "Specify subscription name", required = true)
                               @PathParam("subscription") String subscription,
                               @ApiParam(value = "Specify subscription name", required = true)
                               @PathParam("consumerId") String consumerId,
                               ConsumeMessageRequest consumeMessageRequest) {
        validateTopicName(tenant, namespace, encodedTopic);
        consumeMessages(asyncResponse, consumeMessageRequest, subscription, consumerId);
    }
}
