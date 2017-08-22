package com.bigcrab.infra.kafka.client;

import com.bigcrab.infra.kafka.client.producer.KafkaTemplateFactory;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Created by sky on 2017/8/22.
 */
public class ProducerMain {

    public static void main(String[] args) {
        KafkaTemplateFactory kafkaTemplateFactory = new KafkaTemplateFactory();
        for (int i = 0; i < 10000; ++i) {
            KafkaTemplate<Integer, String> template = kafkaTemplateFactory.createTemplate("test");
            template.sendDefault(i, String.valueOf(i));
            template.flush();

        }
    }
}
