package org.kbit.flume.utils;

import org.eclipse.paho.client.mqttv3.*;
import org.kbit.flume.conf.MqttConfiguration;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.kbit.flume.source.MqttSource.getMqttConnectOptions;

public class MqttSourceClientPool {

    // A list to hold MqttSourceClient instances
    private final List<MqttSourceClient> mqttSourceClients;
    private int currentIndex = 0; // To keep track of the next client to be used
    private final Logger logger;

    /**
     * Constructor to initialize the pool with a given size and configuration.
     * @param poolSize The number of MqttSourceClient instances to create.
     * @param config The MqttConfiguration to use for each client.
     * @param logger The logger instance for logging information.
     */
    public MqttSourceClientPool(int poolSize, MqttConfiguration config, Logger logger) {
        this.logger = logger;
        this.mqttSourceClients = new ArrayList<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            // Create a new MqttClient and connect it
            MqttClient mqttClient = connectAndSubscribe(config, config.getTopic(), logger);

            // Create a new ScheduledExecutorService for this client
            ScheduledExecutorService scheduler = startConnectionCheckScheduler(mqttClient, logger);

            // Add the MqttSourceClient to the pool
            MqttSourceClient sourceClient = new MqttSourceClient(mqttClient, scheduler);
            mqttSourceClients.add(sourceClient);
        }
    }

    /**
     * Constructor that also sets a callback for all clients in the pool.
     * @param poolSize The number of MqttSourceClient instances to create.
     * @param config The MqttConfiguration to use for each client.
     * @param callback The MqttCallback to set for all clients.
     * @param logger The logger instance for logging information.
     */
    public MqttSourceClientPool(int poolSize, MqttConfiguration config, MqttCallback callback, Logger logger) {
        this.logger = logger;
        this.mqttSourceClients = new ArrayList<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            // Create a new MqttClient and connect it
            MqttClient mqttClient = connectAndSubscribe(config, config.getTopic(), callback, logger);

            // Create a new ScheduledExecutorService for this client
            ScheduledExecutorService scheduler = startConnectionCheckScheduler(mqttClient, logger);

            // Add the MqttSourceClient to the pool
            MqttSourceClient sourceClient = new MqttSourceClient(mqttClient, scheduler);
            mqttSourceClients.add(sourceClient);
        }
    }

    /**
     * Helper method to connect and subscribe an MqttClient using the provided configuration.
     * @param config The MqttConfiguration for the client.
     * @param topic The topic to subscribe to.
     * @param logger The logger instance for logging connection information.
     * @return The connected MqttClient instance.
     */
    private MqttClient connectAndSubscribe(MqttConfiguration config, String topic, Logger logger) {
        try {
            MqttClient mqttClient = new MqttClient(config.getBrokerUrl(), MqttClient.generateClientId());

            MqttConnectOptions options = getMqttConnectOptions(config);
            mqttClient.connect(options);

            if (!topic.isEmpty()) {
                mqttClient.subscribe(topic, config.getQos());
            }

            logger.info("Connected and subscribed client: {} to topic: {}",
                    mqttClient.getClientId(), topic);
            return mqttClient;
        } catch (Exception e) {
            logger.error("Failed to connect and subscribe to MQTT broker", e);
            throw new RuntimeException("Failed to connect and subscribe to MQTT broker", e);
        }
    }

    /**
     * Helper method to connect and subscribe an MqttClient with a callback.
     * @param config The MqttConfiguration for the client.
     * @param topic The topic to subscribe to.
     * @param callback The MqttCallback to set for the client.
     * @param logger The logger instance for logging connection information.
     * @return The connected MqttClient instance.
     */
    private MqttClient connectAndSubscribe(MqttConfiguration config, String topic, MqttCallback callback, Logger logger) {
        try {
            MqttClient mqttClient = new MqttClient(config.getBrokerUrl(), MqttClient.generateClientId());

            // Set the callback before connecting
            mqttClient.setCallback(callback);

            MqttConnectOptions options = getMqttConnectOptions(config);
            mqttClient.connect(options);

            if (!topic.isEmpty()) {
                mqttClient.subscribe(topic, config.getQos());
            }

            logger.info("Connected and subscribed client: {} to topic: {} with callback",
                    mqttClient.getClientId(), topic);
            return mqttClient;
        } catch (Exception e) {
            logger.error("Failed to connect and subscribe to MQTT broker", e);
            throw new RuntimeException("Failed to connect and subscribe to MQTT broker", e);
        }
    }

    /**
     * Set callback for all clients in the pool.
     * @param callback The MqttCallback to set for all clients.
     */
    public synchronized void setCallbackForAllClients(MqttCallback callback) {
        for (MqttSourceClient sourceClient : mqttSourceClients) {
            try {
                MqttClient client = sourceClient.getMqttClient();
                if (client != null) {
                    client.setCallback(callback);
                    logger.debug("Set callback for client: {}", client.getClientId());
                }
            } catch (Exception e) {
                logger.error("Failed to set callback for client", e);
            }
        }
    }

    public ScheduledExecutorService startConnectionCheckScheduler(MqttClient client, Logger logger) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> checkConnection(client, logger), 0, 10, TimeUnit.SECONDS);
        return scheduler;
    }

    private void checkConnection(MqttClient client, Logger logger) {
        logger.debug("Checking connection for client: {}", client.getClientId());
        if (client == null) return;

        if (!client.isConnected()) {
            try {
                client.reconnect();
                logger.info("Reconnected client: {} to MQTT broker", client.getClientId());
            } catch (MqttException e) {
                logger.error("Failed to reconnect client: {} to MQTT broker", client.getClientId(), e);
            }
        }
    }

    /**
     * Get the next MqttSourceClient from the pool using a round-robin strategy.
     * This method is synchronized to ensure thread safety.
     * @return The next MqttSourceClient to be used.
     */
    public synchronized MqttSourceClient getNextSourceClient() {
        if (mqttSourceClients.isEmpty()) {
            throw new IllegalStateException("No MqttSourceClients available in the pool.");
        }
        MqttSourceClient client = mqttSourceClients.get(currentIndex);
        currentIndex = (currentIndex + 1) % mqttSourceClients.size();
        return client;
    }

    /**
     * Get the size of the client pool.
     * @return The number of clients in the pool.
     */
    public int getPoolSize() {
        return mqttSourceClients.size();
    }

    /**
     * Get all clients in the pool (for monitoring purposes).
     * @return A copy of the client list.
     */
    public synchronized List<MqttSourceClient> getAllClients() {
        return new ArrayList<>(mqttSourceClients);
    }

    /**
     * Shutdown all clients and their schedulers in the pool.
     */
    public synchronized void shutdownPool() {
        logger.info("Shutting down MQTT client pool with {} clients", mqttSourceClients.size());

        for (int i = 0; i < mqttSourceClients.size(); i++) {
            MqttSourceClient sourceClient = mqttSourceClients.get(i);
            try {
                MqttClient client = sourceClient.getMqttClient();
                ScheduledExecutorService scheduler = sourceClient.getExecutorService();

                if (client != null && client.isConnected()) {
                    client.disconnect();
                    logger.info("Disconnected client: {}", client.getClientId());
                }
                if (scheduler != null && !scheduler.isShutdown()) {
                    scheduler.shutdown();
                    try {
                        if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                            scheduler.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        scheduler.shutdownNow();
                        Thread.currentThread().interrupt();
                    }
                    logger.debug("Shutdown scheduler for client {}", i + 1);
                }
            } catch (Exception e) {
                logger.error("Error shutting down client {}: {}", i + 1, e.getMessage(), e);
            }
        }

        logger.info("MQTT client pool shutdown completed");
    }
}