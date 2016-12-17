package com.github.andy.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * KafkaConsumer
 *
 * @author andy
 * @date 2016年12月18日 上午1:09
 */
public final class KafkaConsumer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    private org.apache.kafka.clients.consumer.KafkaConsumer<K, V> consumer;

    private Properties properties = new Properties();

    public KafkaConsumer(Properties props) {
        if (props != null) {
            this.properties = props;
            initConsumer();
        } else {
            throw new RuntimeException("KafkaConsumer init error. props is null");
        }
    }

    public KafkaConsumer(String servers, String groupId) {
        //props.put("auto.offset.reset", "earliest");
        properties.put("bootstrap.servers", servers);// servers, eg: host:port,host:port
        properties.put("group.id", groupId);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        initConsumer();
    }

    private void initConsumer() {
        try {
            this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<K, V>(this.properties);
        } catch (Exception e) {
            throw new RuntimeException("KafkaConsumer init error.", e);
        }
    }

    /**
     * 消费Kafka数据
     *
     * @param topics
     * @param msgConsumer 接收函数
     */
    public void consumer(List<String> topics, IMessageConsumer msgConsumer) {
        try {
            this.consumer.subscribe(topics);
            while (true) {
                ConsumerRecords<K, V> records = this.consumer.poll(1000);
                for (ConsumerRecord<K, V> record : records) {
                    msgConsumer.receiveMessage(record.value().toString());
                }
            }
        } catch (Exception e) {
            logger.error("KafkaConsumer consuming error.", e);
        } finally {
            this.consumer.close();
        }
    }

    /**
     * 消费Kafka数据
     *
     * @param topic
     * @param msgConsumer 接收函数
     */
    public void consumer(String topic, IMessageConsumer msgConsumer) {
        List<String> topics = new ArrayList<String>(1);
        topics.add(topic);
        consumer(topics, msgConsumer);
    }

    public void consumer(int threadCount, String topic, IMessageConsumer msgPool) {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        final List<MultiConsumer> consumers = new ArrayList<MultiConsumer>();
        for (int i = 0; i < threadCount; i++) {
            MultiConsumer consumer = new MultiConsumer(topic, msgPool);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                consumers.forEach(MultiConsumer::shutdown);
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    logger.error("KafkaConsumer close error.", e);
                }
            }
        });
    }

    public class MultiConsumer implements Runnable {

        private String topic;

        private IMessageConsumer msgConsumer;

        public MultiConsumer(String topic, IMessageConsumer msgConsumer) {
            this.topic = topic;
            this.msgConsumer = msgConsumer;
        }

        @Override
        public void run() {
            try {
                consumer(topic, msgConsumer);
            } catch (Exception e) {
                logger.error("KafkaConsumer error.", e);
            } finally {
                consumer.close();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }

}
