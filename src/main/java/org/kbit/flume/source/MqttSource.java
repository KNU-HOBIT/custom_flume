package org.kbit.flume.source;

import org.apache.flume.*;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.eclipse.paho.client.mqttv3.*;
import org.jetbrains.annotations.NotNull;
import org.kbit.flume.conf.MqttConfiguration;
import org.kbit.flume.utils.MqttSourceClient;
import org.kbit.flume.utils.MqttSourceClientPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MqttSource extends AbstractSource implements Configurable, EventDrivenSource, MqttCallback {
    protected final Logger logger = LoggerFactory.getLogger(MqttSource.class);

    // Use the MqttSourceClientPool instead of managing clients individually
    protected MqttSourceClientPool clientPool;
    protected MqttConfiguration mqttConfiguration;
    private int threadPoolSize;
    private ExecutorService executorService;

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

            this.threadPoolSize = context.getInteger("threadPoolSize", 2);

            validateConfiguration(this.mqttConfiguration);

        } catch (Exception e) {
            logger.error("Error during configuration", e);
            throw new RuntimeException("Configuration error: " + e.getMessage(), e);
        }
    }

    @Override
    public synchronized void start() {
        try {
            // Initialize the ExecutorService for async message processing
            this.executorService = Executors.newFixedThreadPool(threadPoolSize);

            // Create the MqttSourceClientPool with this instance as the callback
            this.clientPool = new MqttSourceClientPool(threadPoolSize, mqttConfiguration, this, logger);

            this.logConfiguration();

            super.start();

            logger.info("MQTT Source started successfully with {} clients", threadPoolSize);

        } catch (Exception e) {
            logger.error("Failed to start MQTT source", e);
            throw new RuntimeException("Failed to start MQTT source", e);
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        CompletableFuture.runAsync(() -> {
            try {
                // Get the next available client from the pool
                MqttSourceClient sourceClient = clientPool.getNextSourceClient();

                // Process message
                logger.debug("Processing message from topic {} using client {}",
                        topic, sourceClient.getMqttClient().getClientId());

                Event event = EventBuilder.withBody(message.getPayload());
                getChannelProcessor().processEvent(event);

            } catch (Exception e) {
                logger.error("Error processing MQTT message", e);
            }
        }, executorService);
    }

    @Override
    public synchronized void stop() {
        try {
            // Shutdown the client pool - this handles all client disconnections and scheduler shutdowns
            if (clientPool != null) {
                clientPool.shutdownPool();
                logger.info("MQTT client pool shut down successfully");
            }

            // Shutdown the main executor service
            if (executorService != null && !executorService.isShutdown()) {
                executorService.shutdown();
                try {
                    if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                        executorService.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executorService.shutdownNow();
                    Thread.currentThread().interrupt();
                }
                logger.info("Main executor service shut down successfully");
            }

        } catch (Exception e) {
            logger.error("Error during MQTT source shutdown", e);
        }

        super.stop();
    }

    @Override
    public void connectionLost(Throwable cause) {
        logger.warn("MQTT connection lost", cause);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // Not used in this context
    }

    @NotNull
    public static MqttConnectOptions getMqttConnectOptions(MqttConfiguration config) {
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

    public void validateConfiguration(MqttConfiguration config) {
        if (config.getBrokerUrl() == null || config.getBrokerUrl().isEmpty()) {
            throw new IllegalArgumentException("brokerUrl cannot be empty");
        }

        if (config.getTopic() == null || config.getTopic().isEmpty()) {
            throw new IllegalArgumentException("topic cannot be empty");
        }
    }

    public void logConfiguration() {
        this.logger.info("MQTT Source Configuration: {}", this.mqttConfiguration.toString());
        this.logger.info("Thread pool size: {}", this.threadPoolSize);
    }
}