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
    protected static final Logger logger = LoggerFactory.getLogger(MqttSource.class);
    private ScheduledExecutorService scheduler;
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
        if (this.mqttConfiguration == null) {
            throw new IllegalStateException("MQTT configuration is not properly set. Make sure configure() is called before start().");
        }

        logConfiguration();

        connectAndSubscribe(this.mqttConfiguration);

        this.scheduler = Executors.newScheduledThreadPool(1);
        this.scheduler.scheduleAtFixedRate(this::checkConnection, 0, 10, TimeUnit.SECONDS);

        super.start();
    }


    private void connectAndSubscribe(MqttConfiguration config) {
        try {
            this.mqttClient = new MqttClient(config.getBrokerUrl(), MqttClient.generateClientId());
            this.mqttClient.setCallback(this);

            MqttConnectOptions options = getMqttConnectOptions(config);

            this.mqttClient.connect(options);

            if (config.getQos() == 0) {
                this.mqttClient.subscribe(config.getTopic());
            } else {
                this.mqttClient.subscribe(config.getTopic(), config.getQos());
            }

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

    // 새로운 getMqttClient 메서드 추가
    protected MqttClient getMqttClient() {
        return mqttClient;
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
    public void connectionLost(Throwable cause) {
        logger.error("MQTT connection lost", cause);
        checkConnection();
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // Not used in this context
    }

    private void checkConnection() {
        logger.info("Check connection ...");
        if (this.mqttClient == null) return;

        if (!this.mqttClient.isConnected()) {
            try {
                this.mqttClient.reconnect();
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
        logger.info(this.mqttConfiguration.toString());
    }

}
