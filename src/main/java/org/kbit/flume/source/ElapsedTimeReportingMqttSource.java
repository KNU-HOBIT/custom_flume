package org.kbit.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.kbit.flume.utils.AckResult;

import java.util.HashMap;
import java.util.Map;

public class ElapsedTimeReportingMqttSource extends MqttSource{
    /**
     * This class extends MqttSource and handles receiving, acknowledging, and timestamping messages
     * from an MQTT broker before forwarding them as Flume events.<p>
     *
     * Key Responsibilities:
     * 1. Receives messages from an MQTT topic and sends an acknowledgment to a returnTopic.
     * 2. Adds a timestamp to the message and creates a Flume event with message ID and timestamp headers.
     * 3. Forwards the created event to the Flume channel processor for further processing.
     */

    @Override
    public void configure(Context context) {
        super.configure(context);
        String returnTopic = context.getString("returnTopic");
        if (returnTopic == null || returnTopic.isEmpty()) {
            throw new IllegalArgumentException("returnTopic cannot be null or empty");
        }
        super.mqttConfiguration.setReturnTopic(returnTopic);
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        // Add a timestamp to the event
        AckResult result = extractMessageContentAndSendAck(super.mqttConfiguration.getReturnTopic(), message);

        Event event = createEventWithTimestamp(result);

        // Process the event
        getChannelProcessor().processEvent(event);
        logger.debug("Message arrived from topic " + topic + ": " + result.getMessageID());
    }
    private AckResult extractMessageContentAndSendAck(String returnTopic, MqttMessage receivedMessage) {
        if (returnTopic == null || returnTopic.isEmpty()) {
            logger.warn("Return topic is not set. Skipping ACK.");
            return new AckResult(null, new byte[0]);
        }
        try {
            // 수신한 메시지에서 메시지 ID와 createdTime를 추출 (구분자 "|"를 사용하여 분리)
            String payload = new String(receivedMessage.getPayload());
            String[] parts = payload.split("\\|", 3);
            if (parts.length < 3) {
                logger.error("Invalid message format: " + payload);
                return new AckResult(null, new byte[0]);
            }
            String messageID = parts[0];
            String createdTime = parts[1];

            // 메시지 ID와 createdTime 을 구분자로 구분하여 전송
            String ackPayload = messageID + "|" + createdTime;
            MqttMessage ackMessage = new MqttMessage(ackPayload.getBytes());
            super.mqttClient.publish(returnTopic, ackMessage);
            logger.debug("Sent ACK to return topic: " + returnTopic + " with message ID: " + messageID + " and createdTime: " + createdTime);

            return new AckResult(messageID, parts[2].getBytes());
        } catch (MqttException e) {
            logger.error("Failed to send ACK to return topic: " + returnTopic, e);
        }
        return new AckResult(null, new byte[0]);
    }

    private Event createEventWithTimestamp(AckResult result) {
        Map<String, String> headers = new HashMap<>();
        headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
        headers.put("msgId", result.getMessageID());
        return EventBuilder.withBody(result.getMessageContent(), headers);
    }

    @Override
    public synchronized void stop() {
        super.stop();
    }

}