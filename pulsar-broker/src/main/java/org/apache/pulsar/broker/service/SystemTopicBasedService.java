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
package org.apache.pulsar.broker.service;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.events.BaseEvent;
import org.apache.pulsar.common.util.FutureUtil;

public interface SystemTopicBasedService<T> {

    SystemTopicBasedService DISABLED = new SystemTopicBasedServiceDisabled();

    CompletableFuture<MessageId> publishMessageAsync(T event);

    MessageId publishMessage(T event) throws Exception;

    CompletableFuture<Message> readMessageAsync();

    Message readMessage() throws Exception;

    SystemTopicClient.Writer getWriter() throws PulsarClientException;

    SystemTopicClient.Reader getReader() throws PulsarClientException;

    void close() throws Exception;

    CompletableFuture<Void> closeAsync();

    class SystemTopicBasedServiceDisabled<T extends BaseEvent> implements SystemTopicBasedService<T> {
        @Override
        public CompletableFuture<MessageId> publishMessageAsync(T event) {
            return FutureUtil.failedFuture(new UnsupportedOperationException("System topic based service disabled."));
        }

        @Override
        public MessageId publishMessage(T event) throws Exception {
            return null;
        }

        @Override
        public CompletableFuture<Message> readMessageAsync() {
            return FutureUtil.failedFuture(new UnsupportedOperationException("System topic based service disabled."));
        }

        @Override
        public Message readMessage() throws Exception {
            return null;
        }

        @Override
        public SystemTopicClient.Writer getWriter() {
            return null;
        }

        @Override
        public SystemTopicClient.Reader getReader() {
            return null;
        }

        @Override
        public void close() throws Exception{
            // No-op
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            // No-op
            return CompletableFuture.completedFuture(null);
        }
    }
}
