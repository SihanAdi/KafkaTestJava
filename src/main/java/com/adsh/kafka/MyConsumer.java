package com.adsh.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class MyConsumer {
    private final static String TOPIC_NAME = "my-replicated-topic";
    private final static String CONSUMER_GROUP_NAME = "testGroup";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "192.168.64.4:9092,192.168.64.4:9093,192.168.64.4:9094");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //#######################################################################

        /*
        Send the consumer group, topic, and offset to the kafka cluster's _consumer_offsets
         */
        /*
        Automatic submission
        It may lose the message because it commits the offset before the message is used.
        Because the user client may be down.
         */
//        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // default is true
//        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); // the interval od Automatic submission

        /*
        Manually submission
         */
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //*****************************************************************

        //Maximum number of messages in a poll
//        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        //If the interval between two polls is more than 30 seconds,
        //Kafka considers this consumer to be too weak to consume and kicks him out of the consumption group,
        //dividing the partition among other consumers.
//        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30 * 1000);

        //*****************************************************************

        //The interval between heartbeats sent by the consumer
//        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000);

        //If Kafka does not receive a heartbeat from a consumer for more than 10 seconds,
        //kick it out of the consumer group,
        // perform a rebalance, and divide the partition among other consumers.
//        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10 * 1000);

        //*****************************************************************
        /*
        When the consumer group is a new group
        latest: Only receive messages sent to the topic after you start it yourself (Default)
        earliest: Consume from the beginning for the first time,
                  and continue to consume according to the offset record in the future
         */
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");



        //#######################################################################


        //Creating a consumer client
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        //Consumer subscribes the topic list
        kafkaConsumer.subscribe(Arrays.asList(TOPIC_NAME));

        //Specify the partition to be used
//        kafkaConsumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME, 0)));

        //Get all messages from the first one
//        kafkaConsumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME, 0)));
//        kafkaConsumer.seekToBeginning(Arrays.asList(new TopicPartition(TOPIC_NAME, 0)));

        //Specific the offset to be used
//        kafkaConsumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME, 0)));
//        kafkaConsumer.seek(new TopicPartition(TOPIC_NAME, 0), 6);

        //Specific the time to be used
//        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(TOPIC_NAME);
//        long fetchDataTime = new Date().getTime() - 1000 * 60 * 60 * 3;
//        Map<TopicPartition, Long> topicPartitionLongHashMap = new HashMap<>();
//        for (PartitionInfo partitionInfo : partitionInfos) {
//            topicPartitionLongHashMap.put(new TopicPartition(TOPIC_NAME, partitionInfo.partition()), fetchDataTime);
//        }
//        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = kafkaConsumer.offsetsForTimes(topicPartitionLongHashMap);
//        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : topicPartitionOffsetAndTimestampMap.entrySet()) {
//            TopicPartition topicPartition = entry.getKey();
//            OffsetAndTimestamp offsetAndTimestamp = entry.getValue();
//            if (topicPartition == null || offsetAndTimestamp == null){
//                continue;
//            }
//            long offset = offsetAndTimestamp.offset();
//            System.out.println("partition-" + topicPartition.partition() + " | offset" + offset);
//            System.out.println();
//
//            if (offsetAndTimestamp != null){
//                kafkaConsumer.assign(Arrays.asList(topicPartition));
//                kafkaConsumer.seek(topicPartition, offset);
//            }
//        }


        while (true){
            /*
            poll() API: Long polling for getting messages
             */
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Received message: partition = %d, offset = %d, key = %s, value = %s%n",
                        record.partition(), record.offset(), record.key(), record.value());
            }

             /*
            Manually submission offset
             */
            // All the messages are used
            if (records.count() > 0){
                /*
                Manually synchronized commits (Recommended)
                 */
                kafkaConsumer.commitSync();//===blocked==== commit success

                /*
                Manually Asynchronous commits
                 */
//                kafkaConsumer.commitAsync(new OffsetCommitCallback() {
//                    @Override
//                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
//                        if (e != null){
//                            System.err.println("Commit failed for " + map);
//                            System.err.println("Commit failed exception: " + e.getStackTrace());
//                        }
//                    }
//                });
            }
        }


    }
}
