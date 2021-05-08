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
package org.apache.pulsar.broker.systopic;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.events.LoadBalanceStatsEvent;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class LoadBalanceStatsSystemTopicClient extends SystemTopicClientBase<LoadBalanceStatsEvent>{

    public LoadBalanceStatsSystemTopicClient(PulsarClient client, TopicName topicName) {
        super(client, topicName);
    }

    @Override
    protected CompletableFuture<Writer<LoadBalanceStatsEvent>> newWriterAsyncInternal() {
        return client.newProducer(Schema.AVRO(LoadBalanceStatsEvent.class))
            .topic(topicName.toString())
            .createAsync()
            .thenCompose(producer -> {
                log.info("[hangc] {} A new writer is created", topicName);
                if (log.isDebugEnabled()) {
                    log.debug("[{}] A new writer is created", topicName);
                }

                return CompletableFuture.completedFuture(new LoadBalanceStatsWriter(producer, this));
            });
    }

    @Override
    protected CompletableFuture<Reader<LoadBalanceStatsEvent>> newReaderAsyncInternal() {
        return client.newReader(Schema.AVRO(LoadBalanceStatsEvent.class))
                .startMessageId(MessageId.latest)
            .topic(topicName.toString())
            .createAsync()
            .thenCompose(reader -> {
                log.info("[hangc] {} A new reader is created. ", topicName);
                if (log.isDebugEnabled()) {
                    log.debug("[{}] A new reader is created", topicName);
                }

                return CompletableFuture.completedFuture(new LoadBalanceStatsReader(reader, this));
            });
    }

    private static class LoadBalanceStatsWriter implements Writer<LoadBalanceStatsEvent> {
        private final Producer<LoadBalanceStatsEvent> producer;
        private final SystemTopicClient<LoadBalanceStatsEvent> systemTopicClient;

        private LoadBalanceStatsWriter(Producer<LoadBalanceStatsEvent> producer,
                                       SystemTopicClient<LoadBalanceStatsEvent> systemTopicClient) {
            this.producer = producer;
            this.systemTopicClient = systemTopicClient;
        }

        @Override
        public MessageId write(LoadBalanceStatsEvent event) throws PulsarClientException {
            return producer.newMessage().key(getEventKey(event)).value(event).send();
        }

        @Override
        public CompletableFuture<MessageId> writeAsync(LoadBalanceStatsEvent event) {
            return producer.newMessage().key(getEventKey(event)).value(event).sendAsync();
        }

        private String getEventKey(LoadBalanceStatsEvent event) {
            // TODO
            return "";
        }

        @Override
        public void close() throws IOException {
            this.producer.close();
            systemTopicClient.getWriters().remove(LoadBalanceStatsWriter.this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return producer.closeAsync().thenCompose(v -> {
                systemTopicClient.getWriters().remove(LoadBalanceStatsWriter.this);
                return CompletableFuture.completedFuture(null);
            });
        }

        @Override
        public SystemTopicClient<LoadBalanceStatsEvent> getSystemTopicClient() {
            return systemTopicClient;
        }
    }

    private static class LoadBalanceStatsReader implements Reader<LoadBalanceStatsEvent> {
        private final org.apache.pulsar.client.api.Reader<LoadBalanceStatsEvent> reader;
        private final SystemTopicClient<LoadBalanceStatsEvent> systemTopicClient;

        private LoadBalanceStatsReader(org.apache.pulsar.client.api.Reader<LoadBalanceStatsEvent> reader,
                                       SystemTopicClient<LoadBalanceStatsEvent> systemTopicClient) {
            this.reader = reader;
            this.systemTopicClient = systemTopicClient;
        }

        @Override
        public Message<LoadBalanceStatsEvent> readNext() throws PulsarClientException {
            return reader.readNext();
        }

        @Override
        public CompletableFuture<Message<LoadBalanceStatsEvent>> readNextAsync() {
            return reader.readNextAsync();
        }

        @Override
        public Message<LoadBalanceStatsEvent> readNext(int timeout, TimeUnit timeUnit) throws PulsarClientException {
            return reader.readNext(timeout, timeUnit);
        }

        @Override
        public boolean hasMoreEvents() throws PulsarClientException {
            return reader.hasMessageAvailable();
        }

        @Override
        public CompletableFuture<Boolean> hasMoreEventsAsync() {
            return reader.hasMessageAvailableAsync();
        }

        @Override
        public void close() throws IOException {
            this.reader.close();
            systemTopicClient.getReaders().remove(LoadBalanceStatsReader.this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return reader.closeAsync().thenCompose(v -> {
                systemTopicClient.getReaders().remove(LoadBalanceStatsReader.this);
                return CompletableFuture.completedFuture(null);
            });
        }

        @Override
        public SystemTopicClient<LoadBalanceStatsEvent> getSystemTopicClient() {
            return systemTopicClient;
        }
    }
}
