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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.events.BaseEvent;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

// TODO support multi topic and event publish and read.
@Slf4j
public class SystemTopicBasedServiceImpl<T extends BaseEvent> implements SystemTopicBasedService<T> {
    private final PulsarService pulsarService;
    private volatile NamespaceEventsSystemTopicFactory namespaceEventsSystemTopicFactory;

    private SystemTopicClient.Writer writer;
    private SystemTopicClient.Reader reader;
    private final SystemTopicClient client;

    public SystemTopicBasedServiceImpl(PulsarService pulsarService, TopicName topicName, EventType eventType) {
        this.pulsarService = pulsarService;
        createSystemTopicFactoryIfNeeded();
        client = namespaceEventsSystemTopicFactory.createSystemTopicClient(topicName.getNamespaceObject(), eventType);
    }

    @Override
    public CompletableFuture<MessageId> publishMessageAsync(T event) {
        try {
            return createOrGetWriter().writeAsync(event);
        } catch (Exception e) {
            log.error("Failed to write event. ", e);
            return null;
        }
    }

    @Override
    public MessageId publishMessage(T event) throws Exception {
        return publishMessageAsync(event).get();
    }

    @Override
    public CompletableFuture<Message> readMessageAsync() {
        try {
            return createOrGetReader().readNextAsync();
        } catch (Exception e) {
            log.error("Failed to reader msg. ", e);
            return null;
        }
    }

    @Override
    public Message readMessage() throws Exception {
        return readMessageAsync().get();
    }

    @Override
    public SystemTopicClient.Writer getWriter() throws PulsarClientException{
        return createOrGetWriter();
    }

    @Override
    public SystemTopicClient.Reader getReader() throws PulsarClientException{
        return createOrGetReader();
    }

    private SystemTopicClient.Writer createOrGetWriter() throws PulsarClientException {
        if (writer == null) {
            synchronized (this) {
                if (writer == null) {
                    try {
                        writer = client.newWriter();
                    } catch (PulsarClientException e) {
                        log.error("Failed to create system topic writer. ", e);
                        throw new PulsarClientException(e);
                    }
                }
            }
        }

        return writer;
    }

    private SystemTopicClient.Reader createOrGetReader() throws PulsarClientException {
        if (reader == null) {
            synchronized (this) {
                if (reader == null) {
                    try {
                        reader = client.newReader();
                    } catch (PulsarClientException e) {
                        log.error("Failed to create system topic reader. ", e);
                        throw new PulsarClientException(e);
                    }
                }
            }
        }

        return reader;
    }

    private void createSystemTopicFactoryIfNeeded() {
        if (namespaceEventsSystemTopicFactory == null) {
            synchronized (this) {
                if (namespaceEventsSystemTopicFactory == null) {
                    try {
                        namespaceEventsSystemTopicFactory =
                            new NamespaceEventsSystemTopicFactory(pulsarService.getClient());
                    } catch (PulsarServerException e) {
                        log.error("Failed to create namespace event system topic factory.", e);
                    }
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        closeAsync().get();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        futures.add(writer.closeAsync());
        futures.add(reader.closeAsync());
        futures.add(client.closeAsync());
        return FutureUtil.waitForAll(futures);
    }
}
