package org.kbit.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.kbit.flume.conf.MqttConfiguration;
import org.kbit.flume.utils.MessageTuple;
import org.kbit.flume.utils.MqttSourceClient;
import org.kbit.flume.utils.MqttSourceClientPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ElapsedTimeReportingMqttSource extends MqttSource {
    /**
     * This class extends MqttSource and handles receiving, acknowledging, and timestamping messages
     * from an MQTT broker before forwarding them as Flume events.<p>
     *
     * Key Responsibilities:
     * 1. Receives messages from an MQTT topic and sends an acknowledgment to a returnTopic.<p>
     * 2. Adds a timestamp to the message and creates a Flume event with message ID and timestamp headers.<p>
     * 3. Forwards the created event to the Flume channel processor for further processing.<p>
     */
    private final Logger logger = LoggerFactory.getLogger(ElapsedTimeReportingMqttSource.class);

    // Pool for ACK clients (separate from the main receiving clients)
    private MqttSourceClientPool ackClientPool;
    private MqttConfiguration ackMqttConfiguration;
    private ExecutorService ackExecutorService;
    private int threadPoolSize;

    @Override
    public void configure(Context context) {
        String returnTopic = context.getString("returnTopic");
        String brokerUrl4Send = context.getString("brokerUrl4Send");
        this.threadPoolSize = context.getInteger("threadPoolSize", 1);

        if (returnTopic == null || returnTopic.isEmpty() || brokerUrl4Send == null || brokerUrl4Send.isEmpty()) {
            throw new IllegalArgumentException("returnTopic & sendBrokerUrl cannot be null or empty");
        }

        // Configure the parent class first
        super.configure(context);

        try {
            // Create separate configuration for ACK clients
            this.ackMqttConfiguration = (MqttConfiguration) super.mqttConfiguration.clone();
            this.ackMqttConfiguration.setReturnTopic(returnTopic);
            this.ackMqttConfiguration.setBrokerUrl(brokerUrl4Send);
            // ACK clients don't need to subscribe to any topic (empty topic)
            this.ackMqttConfiguration.setTopic("");

        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Clone not supported for mqttConfiguration", e);
        }

        // Initialize executor service for ACK operations
        this.ackExecutorService = Executors.newFixedThreadPool(threadPoolSize);
    }

    @Override
    public synchronized void start() {
        // Start the parent class (this will create the receiving client pool)
        super.start();

        try {
            // Create the ACK client pool (no callback needed as these are for publishing only)
            this.ackClientPool = new MqttSourceClientPool(threadPoolSize, ackMqttConfiguration, logger);

            logger.info("ElapsedTimeReportingMqttSource started with {} ACK clients", threadPoolSize);
            this.logConfiguration();

        } catch (Exception e) {
            logger.error("Failed to start ElapsedTimeReportingMqttSource", e);
            throw new RuntimeException("Failed to start ElapsedTimeReportingMqttSource", e);
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        CompletableFuture.runAsync(() -> {
            try {
                // Get the next available ACK client from the pool
                MqttSourceClient ackSourceClient = ackClientPool.getNextSourceClient();
                MqttClient ackClient = ackSourceClient.getMqttClient();

                processMessage(topic, message, ackClient);

            } catch (Exception e) {
                logger.error("Error processing message in ElapsedTimeReportingMqttSource", e);
            }
        }, ackExecutorService);
    }

    private void processMessage(String topic, MqttMessage message, MqttClient mqttClient4Ack) {
        try {
            MessageTuple tuple = extractMessage(message);
            sendSimpleAck(tuple.getMessageID(), mqttClient4Ack);
            Event event = createEventWithTimestamp(tuple);
            getChannelProcessor().processEvent(event);
            logger.debug("Message processed from topic {}: {}", topic, tuple.getMessageID());
        } catch (Exception e) {
            logger.error("Error processing message", e);
        }
    }

    // Method to extract message ID and data as a Map
    private MessageTuple extractMessage(MqttMessage receivedMessage) {
        // Convert the payload to a string
        String payload = new String(receivedMessage.getPayload());

        // Split the payload into two parts
        String[] result = payload.split("\\|", 2);

        // Initialize the message ID and data
        String messageID = result[0];
        byte[] data;

        // Ensure the payload has at least two parts after splitting
        if (result.length == 2) {
            data = result[1].getBytes(); // Convert the remaining data to bytes
        } else {
            // Handle the case where the payload does not have both parts
            data = new byte[0]; // No data if the delimiter is not present
        }

        // Return a new MessageTuple containing the extracted parts
        return new MessageTuple(messageID, data);
    }

    private void sendSimpleAck(String messageID, MqttClient mqttClient4Ack) {
        if (this.ackMqttConfiguration.getReturnTopic() == null || this.ackMqttConfiguration.getReturnTopic().isEmpty()) {
            logger.warn("Return topic is not set. Skipping ACK.");
            return;
        }
        try {
            mqttClient4Ack.publish(this.ackMqttConfiguration.getReturnTopic(), new MqttMessage(messageID.getBytes()));
            logger.debug("Sent ACK to return topic: {} with message ID: {}",
                    this.ackMqttConfiguration.getReturnTopic(), messageID);
        } catch (MqttException e) {
            logger.error("Failed to send ACK to return topic: {}", this.ackMqttConfiguration.getReturnTopic(), e);
        }
    }

    private Event createEventWithTimestamp(MessageTuple tuple) {
        Map<String, String> headers = new HashMap<>();
        headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
        headers.put("msgId", tuple.getMessageID());
        return EventBuilder.withBody(tuple.getData(), headers);
    }

    @Override
    public synchronized void stop() {
        try {
            // Shutdown the ACK client pool
            if (ackClientPool != null) {
                ackClientPool.shutdownPool();
                logger.info("ACK client pool shut down successfully");
            }

            // Shutdown the ACK executor service
            if (ackExecutorService != null && !ackExecutorService.isShutdown()) {
                ackExecutorService.shutdown();
                try {
                    if (!ackExecutorService.awaitTermination(5, TimeUnit.SECONDS)) {
                        ackExecutorService.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    ackExecutorService.shutdownNow();
                    Thread.currentThread().interrupt();
                }
                logger.info("ACK executor service shut down successfully");
            }

        } catch (Exception e) {
            logger.error("Error during ElapsedTimeReportingMqttSource shutdown", e);
        }

        // Call the superclass stop method (this will handle the receiving client pool)
        super.stop();
    }

    @Override
    public void logConfiguration() {
        super.logConfiguration();
        logger.info("ACK MQTT Configuration: {}", this.ackMqttConfiguration.toString());
        logger.info("ACK client pool size: {}", this.threadPoolSize);
    }
}