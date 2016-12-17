

描述:  简单的kafka客户端


依赖:  kafka version: 0.10.0.1, jdk1.8



消费者使用示例:

     new KafkaConsumer<String, String>(props).consumer(3,"test", s -> doSomething(s));


生产者使用示例:  

     KafkaProducer<String, String> producer = new KafkaProducer<>(props);
     while (true) {
        producer.sendMsg("test", UUID.randomUUID().toString(), (metadata, exception) -> callBack()
     }


