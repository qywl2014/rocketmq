/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.quickstart.myself;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService;
import org.apache.rocketmq.common.message.MessageExt;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * This example shows how to subscribe and consume messages using providing {@link DefaultMQPushConsumer}.
 */
public class ConsumerTest {

    public static void main(String[] args) throws InterruptedException, MQClientException {

        final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-example");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.subscribe("TopicTest2020-1-21", "*");
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);
        consumer.setPullThresholdForQueue(1);

        /*
         *  Register callback to execute on arrival of messages fetched from brokers.
         *  保存 messageListener 到 defaultMQPushConsumer 和 defaultMQPushConsumerImpl 中
         */
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt>/*melot的封装就for循环list调用processor*/ msgs,
                ConsumeConcurrentlyContext context) {
                System.out.println("----------------------------------------------");
                System.out.println("size = "+msgs.size());
                for (MessageExt msg : msgs) {
                    System.out.println("msg = "+new String(msg.getBody(), StandardCharsets.UTF_8));
                }

                ConsumeMessageConcurrentlyService consumeMessageConcurrentlyService = (ConsumeMessageConcurrentlyService)consumer.getDefaultMQPushConsumerImpl().getConsumeMessageService();
                try {
                    Field field = ConsumeMessageConcurrentlyService.class.getDeclaredField("consumeExecutor");
                    field.setAccessible(true);
                    ThreadPoolExecutor threadPoolExecutor=(ThreadPoolExecutor)field.get(consumeMessageConcurrentlyService);
                    System.out.println("consumeExecutor size = "+threadPoolExecutor.getQueue().size());

                    Thread.sleep(50);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
