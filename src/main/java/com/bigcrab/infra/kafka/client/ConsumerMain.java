package com.bigcrab.infra.kafka.client;

import com.bigcrab.infra.kafka.client.consumer.BatchEventHandler;

/**
 * Created by sky on 2017/8/22.
 */
public class ConsumerMain {

    public static void main(String[] args) {
        BatchEventHandler eventHandler = new BatchEventHandler();
        eventHandler.start();
    }
}
