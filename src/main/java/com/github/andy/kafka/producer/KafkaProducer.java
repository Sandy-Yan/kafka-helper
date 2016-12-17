package com.github.andy.kafka.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * KafkaProducer
 *
 * @author andy
 * @date 2016年12月18日 上午1:09
 */
public final class KafkaProducer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    private Producer<K, V> producer;

    private Properties properties = new Properties();

    public KafkaProducer(Properties props) {
        if (props != null) {
            this.properties = props;
            initProducer();
        } else {
            throw new RuntimeException("KafkaProducer init error. props is null");
        }
    }

    public KafkaProducer(String servers, String clientId) {
        properties.put("bootstrap.servers", servers);// 服务器列表,eg:host:port,host:port
        properties.put("client.id", clientId);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "1");// commit方式
        properties.put("retries", "0");// 重试次数
        properties.put("linger.ms", "500");
        properties.put("auto.create.topics.enable", true);

        initProducer();
    }

    private void initProducer() {
        try {
            this.producer = new org.apache.kafka.clients.producer.KafkaProducer<K, V>(this.properties);
        } catch (Exception e) {
            throw new RuntimeException("KafkaProducer init error.", e);
        }
    }

    /**
     * 发送kafka消息
     *
     * @param topic    topic
     * @param msg      发送的数据
     * @param callBack callback
     */
    public void sendMsg(String topic, V msg, ProducerCallBack callBack) {
        this.producer.send(new ProducerRecord<>(topic, msg), callBack);
    }

    /**
     * 发送kafka消息
     *
     * @param topic topic
     * @param msg   发送的数据
     */
    public void sendMsg(String topic, V msg) {
        this.producer.send(new ProducerRecord<>(topic, msg));
    }


    /**
     * 同步发送kafka消息
     *
     * @param topic topic
     * @param msg   发送的数据
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public RecordMetadata sendMsgSyn(String topic, V msg) throws ExecutionException, InterruptedException {
        return this.producer.send(new ProducerRecord<>(topic, msg)).get();
    }

}
