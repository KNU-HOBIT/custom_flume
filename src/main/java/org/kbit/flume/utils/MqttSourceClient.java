package org.kbit.flume.utils;

import org.eclipse.paho.client.mqttv3.MqttClient;

import java.util.concurrent.ScheduledExecutorService;

public class MqttSourceClient {
    private final MqttClient mqttClient;
    private final ScheduledExecutorService executorService;

    public MqttSourceClient(MqttClient mqttClient, ScheduledExecutorService executorService) {
        this.mqttClient = mqttClient;
        this.executorService = executorService;
    }

    public MqttClient getMqttClient() { return mqttClient; }

    public ScheduledExecutorService getExecutorService() {
        return executorService;
    }
}