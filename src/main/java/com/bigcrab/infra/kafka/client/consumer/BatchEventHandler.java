package com.bigcrab.infra.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by sky on 2017/8/22.
 */
public class BatchEventHandler implements BatchMessageListener<Integer, String> {

    private static final Logger logger = LoggerFactory.getLogger(BatchEventHandler.class);

    private static final int QUEUE_DEPTH = 100;
    private static final int POLL_TIMEOUT = 100;
    private static final String GROUP_NAME = "group-2";
    private static final String BOOTSTRAP_SERVER_LIST = "localhost:9092";
    private static final String TOPIC = "test";

    private KafkaMessageListenerContainer<Integer, String> container;

    public BatchEventHandler() {
        ContainerProperties containerProps = new ContainerProperties(TOPIC);
        containerProps.setQueueDepth(QUEUE_DEPTH);
        containerProps.setPollTimeout(POLL_TIMEOUT);
        container = createContainer(containerProps);

        container.setupMessageListener(this);
        container.setBeanName("testAuto");
    }

    public void start() {
        container.start();
    }

    public void stop() {
        container.stop();
    }

    private KafkaMessageListenerContainer<Integer, String> createContainer(ContainerProperties containerProps) {
        Map<String, Object> props = consumerProps();
        DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
        KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf, containerProps);
        return container;
    }

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LIST);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_NAME);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }


    @Override
    public void onMessage(List<ConsumerRecord<Integer, String>> messages) {
        logger.info("messages size is {}", messages.size());
        messages.parallelStream().forEach((message) -> logger.info("receive message: {}", message));
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
