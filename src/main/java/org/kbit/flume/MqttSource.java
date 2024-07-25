package org.kbit.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MqttSource extends AbstractSource implements Configurable, PollableSource, MqttCallback {
    private static final Logger logger = LoggerFactory.getLogger(MqttSource.class);
    private MqttClient mqttClient;
    private Map<String, String> configuration;
    private ScheduledExecutorService scheduler;


    @Override
    public void configure(Context context) {
        try {
            String[] requiredConfigs = {"brokerUrl", "topic"};
            String[] optionalConfigs = {"username", "password", "keepAliveInterval", "connectionTimeout", "cleanSession", "qos"};

            configuration = new HashMap<>();

            for (String key : context.getParameters().keySet()) {
                String value = context.getString(key);
                if (value != null && !value.isEmpty()) {
                    configuration.put(key, value);
                }
            }

            for (String required : requiredConfigs) {
                if (!configuration.containsKey(required) || configuration.get(required).isEmpty()) {
                    throw new IllegalArgumentException(required + " cannot be empty");
                }
            }

            for (String optional : optionalConfigs) {
                if (!configuration.containsKey(optional)) {
                    switch (optional) {
                        case "keepAliveInterval":
                            configuration.put(optional, "60");
                            break;
                        case "connectionTimeout":
                            configuration.put(optional, "30");
                            break;
                        case "cleanSession":
                            configuration.put(optional, "true");
                            break;
                        case "qos":
                            configuration.put(optional, "1");
                            break;
                        case "username":
                            configuration.put(optional, ""); // Default to an empty string
                            break;
                        case "password":
                            configuration.put(optional, ""); // Default to an empty string
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown configuration key: " + optional);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error during configuration", e);
            throw new RuntimeException("Configuration error: " + e.getMessage(), e);
        }
    }

    @Override
    public synchronized void start() {
        if (configuration == null || configuration.isEmpty()) {
            throw new IllegalStateException("MQTT configuration is not properly set. Make sure configure() is called before start().");
        }

        String brokerUrl = configuration.get("brokerUrl");
        String topic = configuration.get("topic");
        String username = configuration.get("username");
        String password = configuration.get("password");
        int keepAliveInterval = Integer.parseInt(configuration.get("keepAliveInterval"));
        int connectionTimeout = Integer.parseInt(configuration.get("connectionTimeout"));
        boolean cleanSession = Boolean.parseBoolean(configuration.get("cleanSession"));
        int qos = Integer.parseInt(configuration.get("qos"));

        if (brokerUrl == null || topic == null || username == null || password == null) {
            throw new IllegalStateException("Required MQTT configuration parameters are missing.");
        }

        try {
            mqttClient = new MqttClient(brokerUrl, MqttClient.generateClientId());
            mqttClient.setCallback(this);

            MqttConnectOptions options = new MqttConnectOptions();
            options.setUserName(username);
            options.setPassword(password.toCharArray());
            options.setKeepAliveInterval(keepAliveInterval);
            options.setConnectionTimeout(connectionTimeout);
            options.setCleanSession(cleanSession);

            mqttClient.connect(options);
            mqttClient.subscribe(topic, qos);
        } catch (MqttException e) {
            throw new RuntimeException("Failed to connect and subscribe to MQTT broker", e);
        }

        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::checkConnection, 0, 10, TimeUnit.SECONDS);

        super.start();
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

    // 새로운 getConfiguration 메서드 추가
    public Map<String, String> getConfiguration() {
        return configuration;
    }
    @Override
    public synchronized void stop() {
        try {
            if (mqttClient != null && mqttClient.isConnected()) {
                mqttClient.disconnect();
                logger.info("Disconnected from MQTT broker");
            }
            if (scheduler != null && !scheduler.isShutdown()) {
                scheduler.shutdown();
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
        if (mqttClient == null) return;

        if (!mqttClient.isConnected()) {
            try {
                mqttClient.reconnect();
                logger.info("Reconnected to MQTT broker");
            } catch (MqttException e) {
                logger.error("Failed to reconnect to MQTT broker", e);
            }
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        return Status.BACKOFF;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 1000L; // 1 second
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 5000L; // 5 seconds
    }
}
