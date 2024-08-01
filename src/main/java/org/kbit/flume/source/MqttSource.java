package org.kbit.flume.source;

import org.apache.flume.*;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.eclipse.paho.client.mqttv3.*;
import org.jetbrains.annotations.NotNull;
import org.kbit.flume.conf.MqttConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MqttSource extends AbstractSource implements Configurable, EventDrivenSource, MqttCallback {
    protected final Logger logger = LoggerFactory.getLogger(MqttSource.class);
    protected ScheduledExecutorService scheduler;
    protected MqttClient mqttClient;
    protected MqttConfiguration mqttConfiguration;

    @Override
    public void configure(Context context) {
        try {
            this.mqttConfiguration = new MqttConfiguration();

            this.mqttConfiguration.setBrokerUrl(context.getString("brokerUrl"));
            this.mqttConfiguration.setTopic(context.getString("topic"));
            this.mqttConfiguration.setUsername(context.getString("username"));
            this.mqttConfiguration.setPassword(context.getString("password"));
            this.mqttConfiguration.setKeepAliveInterval(context.getInteger("keepAliveInterval", 60));
            this.mqttConfiguration.setConnectionTimeout(context.getInteger("connectionTimeout", 30));
            this.mqttConfiguration.setCleanSession(context.getBoolean("cleanSession", true));
            this.mqttConfiguration.setQos(context.getInteger("qos", 0));

            validateConfiguration(this.mqttConfiguration);

        } catch (Exception e) {
            logger.error("Error during configuration", e);
            throw new RuntimeException("Configuration error: " + e.getMessage(), e);
        }
    }

    @Override
    public synchronized void start() {

        this.mqttClient = connectAndSubscribe(this.mqttConfiguration, this.mqttConfiguration.getTopic());

        this.scheduler = startConnectionCheckScheduler(this.mqttClient, this.logger);

        this.logConfiguration();

        super.start();
    }

    public ScheduledExecutorService startConnectionCheckScheduler(MqttClient client, Logger logger) {
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> checkConnection(client, logger), 0, 10, TimeUnit.SECONDS);
        return scheduler;
    }


    protected MqttClient connectAndSubscribe(MqttConfiguration config, String topic) {
        try {
            MqttClient mqttClient = new MqttClient(config.getBrokerUrl(), MqttClient.generateClientId());
            mqttClient.setCallback(this);

            MqttConnectOptions options = getMqttConnectOptions(config);

            mqttClient.connect(options);

            if (topic.isEmpty()) { return mqttClient; }

            if (config.getQos() == 0) {
                mqttClient.subscribe(topic);
            } else {
                mqttClient.subscribe(topic, config.getQos());
            }

            return mqttClient;

        } catch (MqttException e) {
            throw new RuntimeException("Failed to connect and subscribe to MQTT broker", e);
        }
    }

    @NotNull
    static MqttConnectOptions getMqttConnectOptions(MqttConfiguration config) {
        MqttConnectOptions options = new MqttConnectOptions();
        if (config.getUsername() != null) {
            options.setUserName(config.getUsername());
        }
        if (config.getPassword() != null) {
            options.setPassword(config.getPassword().toCharArray());
        }
        options.setKeepAliveInterval(config.getKeepAliveInterval());
        options.setConnectionTimeout(config.getConnectionTimeout());
        options.setCleanSession(config.isCleanSession());
        return options;
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        try {
            Event event = EventBuilder.withBody(message.getPayload());
            getChannelProcessor().processEvent(event);
            logger.debug("Message arrived from topic " + topic + ": " + new String(message.getPayload(), StandardCharsets.UTF_8));
        } catch (Exception e) {
            logger.error("Failed to process the event", e);
            throw new EventDeliveryException("Failed to process the event", e);
        }
    }

    @Override
    public synchronized void stop() {
        try {
            if (this.mqttClient != null && this.mqttClient.isConnected()) {
                this.mqttClient.disconnect();
                logger.info("Disconnected from MQTT broker");
            }
            if (this.scheduler != null && !this.scheduler.isShutdown()) {
                this.scheduler.shutdown();
            }
        } catch (MqttException e) {
            logger.error("Failed to stop MQTT client", e);
        }
        super.stop();
    }
    @Override
    public void connectionLost(Throwable cause) {}

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // Not used in this context
    }

    private void checkConnection(MqttClient client, Logger logger) {
        logger.info("Check connection about "+client+" ...");
        if (client == null) return;

        if (!client.isConnected()) {
            try {
                client.reconnect();
                logger.info("Reconnected to MQTT broker");
            } catch (MqttException e) {
                logger.error("Failed to reconnect to MQTT broker", e);
            }
        }
    }

    public void validateConfiguration(MqttConfiguration config) {
        if (config.getBrokerUrl() == null || config.getBrokerUrl().isEmpty()) {
            throw new IllegalArgumentException("brokerUrl cannot be empty");
        }

        if (config.getTopic() == null || config.getTopic().isEmpty()) {
            throw new IllegalArgumentException("topic cannot be empty");
        }
    }

    public void logConfiguration() {
        this.logger.info(this.mqttConfiguration.toString());
    }

}
