package com.adsh.kafka;

import com.adsh.entity.Order;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyProducer {

    private final static String TOPIC_NAME = "my-replicated-topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1.setting configuration
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "192.168.64.4:9092,192.168.64.4:9093,192.168.64.4:9094");

        //The serialization config
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //以上是必须配置的
        //===============================================================

//        /*
//        ack=0, Return to ack when the message has been sent
//        ack=1 (Default)， The leader receives the message and writes it to the local log and returns the ack
//        ack=-1/all
//         */
//        properties.put(ProducerConfig.ACKS_CONFIG, "1");
//
//        /*
//        Retry if sending fails
//         */
//        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
//
//        /*
//        Retry interval
//         */
//        properties.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 300);
//
//        /*
//
//         */
//        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);//32M
//
//        /*
//
//         */
//        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);//16k
//
//        /*
//
//         */
//        properties.put(ProducerConfig.LINGER_MS_CONFIG, 10);

        //2.Creating the client that sends messages,and pass in parameters
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        //3. Creating the message

        /*
        Not specify the partition to which to send
        
        hash(key)%partitionsNum,
        new ProducerRecord<>(TOPIC_NAME, "my_key"(key), "my_value"(value: Real messages sent));
         */
//        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, "my_key", "my_value");

        /*
        Specify the partition to which to send
         */
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(TOPIC_NAME, 0, "my_key", "my_value");


        //4.send message

        //Synchronized sending
        /*
        RecordMetadata metadata = producer.send(producerRecord).get();//If it does not receive an ACK, it will be blocked
        System.out.println("Sending message results in a synchronous way: "
                + "Topic-" + metadata.topic()
                + " | Partition-" + metadata.partition()
                + " | Offset-" +metadata.offset());
         */

        /*
        Asynchronous sending
        It maybe lost messages
         */
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null){
                    System.err.println("Fail to send: " + e.getStackTrace());
                }
                if (recordMetadata != null){
                    System.out.println("Sending message results in a Asynchronous way: "
                            + "Topic-" + recordMetadata.topic()
                            + " | Partition-" + recordMetadata.partition()
                            + " | Offset-" +recordMetadata.offset());
                }
            }
        });
        Thread.sleep(1000);

    }
}
