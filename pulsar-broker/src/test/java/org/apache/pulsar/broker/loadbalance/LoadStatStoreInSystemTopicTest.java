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
package org.apache.pulsar.broker.loadbalance;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.events.LoadBalanceStatsEvent;
import org.apache.pulsar.common.events.LoadType;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

@Slf4j
public class LoadStatStoreInSystemTopicTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        this.conf.setSystemTopicEnabled(true);
        this.conf.setStoreBundleStatsInSystemTopic(true);
        this.conf.setDefaultNumberOfNamespaceBundles(1);
        this.conf.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        this.conf.setEnablePersistentTopics(true);
        this.conf.setLoadBalancerEnabled(true);
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
        this.resetConfig();
    }

    @Test(timeOut = 30000)
    public void testConsumeMsgFromSystemTopic() throws Exception {
        String topicName = "non-persistent://public/default/__load_balance_stats";

        // step1: check system topic message content, which written by load manager.
        Reader<LoadBalanceStatsEvent> reader =
                pulsarClient.newReader(Schema.AVRO(LoadBalanceStatsEvent.class))
                        .startMessageId(MessageId.latest)
                .topic(topicName)
                .create();

        do {
            try {
                Message<LoadBalanceStatsEvent> msg = reader.readNext(100, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    LoadBalanceStatsEvent event = msg.getValue();
                    LoadType type = event.getLoadType();
                    log.info("[type] LoadType: {}", type);
                    switch (type) {
                        case BROKER_DATA:
                            log.info("[BROKER_DATA] broker: {}, localBrokerData: {}",
                                    event.getBroker(), event.getBrokerData().getJsonString());
                            break;
                        case BUNDLE_DATA:
                            log.info("[BUNDLE_DATA] bundle: {}, bundleData: {}",
                                    event.getBundle(), event.getBundleData().getJsonString());
                            break;
                        case TIME_AVERAGE_BROKER_DATA:
                            log.info("[TIME_AVERAGE_BROKER_DATA] broker: {}, timeAverageBrokerData: {}",
                                    event.getBroker(), event.getTimeAverageBrokerData());
                            break;
                        default:
                            log.info("type not hit, type: {}, msg: {}", type, msg);
                            Assert.fail();
                            break;

                    }
                    break;
                } else {
                    Thread.sleep(1000);
                    log.info("no msg available, sleep 1s");
                }
            } catch (Exception e) {
                log.error("message parse failed. ", e);
                Assert.fail();
            }
        } while (true);

        // step2: check zooKeeper path.
        ZooKeeper zkClient = pulsar.getZkClient();
        final String brokerUrl = pulsar.getAdvertisedAddress() + ":"
                + (conf.getWebServicePort().isPresent() ? conf.getWebServicePort().get()
                : conf.getWebServicePortTls().get());
        final String bundleZPath = ModularLoadManagerImpl.BUNDLE_DATA_ZPATH;
        final String resourceQuotaZPath = ModularLoadManagerImpl.RESOURCE_QUOTA_ZPATH;
        final String timeAverageBrokerZPath = ModularLoadManagerImpl.TIME_AVERAGE_BROKER_ZPATH + "/" + brokerUrl;
        final String brokerZPath = LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + brokerUrl;

        if (zkClient.exists(bundleZPath, null) != null) {
            Assert.fail();
        }

        if (zkClient.exists(resourceQuotaZPath, null) != null) {
            Assert.fail();
        }

        if (zkClient.exists(timeAverageBrokerZPath, null) != null) {
            Assert.fail();
        }

        if (zkClient.exists(brokerZPath, null) != null) {
            String brokerData = new String(zkClient.getData(brokerZPath, false, null));
            Assert.assertEquals(brokerData, "");
        }

        // step3: create persistent topic, produce and consume
        final String produceConsumerTopic = "persistent://public/default/produceConsume";
        final String subName = "sub";
        final int numMsgs = 10;

        admin.topics().createPartitionedTopic(produceConsumerTopic, 1);
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(produceConsumerTopic)
                .subscriptionName(subName)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(produceConsumerTopic)
                .create();

        for (int i = 0; i < numMsgs; ++i) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        for (int i = 0; i < numMsgs; ++i) {
            msg = consumer.receive();
            Assert.assertEquals(new String(msg.getData()), "my-message-" + i);
            consumer.acknowledge(msg);
        }
    }
}
