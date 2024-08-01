package org.kbit.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.kbit.flume.conf.MqttConfiguration;
import org.kbit.flume.utils.AckResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public class ElapsedTimeReportingMqttSource extends MqttSource{
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

    private ScheduledExecutorService scheduler;

    private MqttClient mqttClient4Ack;
    private MqttConfiguration mqttConfiguration;

    @Override
    public void configure(Context context) {
        String returnTopic = context.getString("returnTopic");
        String brokerUrl4Send = context.getString("brokerUrl4Send");
        if (returnTopic == null || returnTopic.isEmpty() || brokerUrl4Send == null || brokerUrl4Send.isEmpty()) {
            throw new IllegalArgumentException("returnTopic & sendBrokerUrl cannot be null or empty");
        }
        super.configure(context);

        // mqttConfiguration의 깊은 복사 수행
        try {
            this.mqttConfiguration = (MqttConfiguration) super.mqttConfiguration.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Clone not supported for mqttConfiguration", e);
        }
        this.mqttConfiguration.setReturnTopic(returnTopic);
        this.mqttConfiguration.setBrokerUrl(brokerUrl4Send);

    }

    @Override
    public void start() {
        // mqttClient4Ack 설정 및 연결 & 연결 체크 스케줄러 시작
        this.mqttClient4Ack = connectAndSubscribe(this.mqttConfiguration, "");
        this.scheduler = startConnectionCheckScheduler(this.mqttClient4Ack, this.logger);
        this.logConfiguration();

        ////////////////////
        super.mqttConfiguration.setBrokerUrl(super.mqttConfiguration.getBrokerUrl());
        super.mqttClient = connectAndSubscribe(super.mqttConfiguration, super.mqttConfiguration.getTopic());
        super.scheduler = startConnectionCheckScheduler(super.mqttClient, super.logger);
        super.logConfiguration();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        // Add a timestamp to the event

        AckResult result = extractMessageContentAndSendAck(this.mqttConfiguration.getReturnTopic(), message);
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

            this.mqttClient4Ack.publish(returnTopic, ackMessage);

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
        try {
            if (this.mqttClient4Ack != null && this.mqttClient4Ack.isConnected()) {
                this.mqttClient4Ack.disconnect();
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
    public void logConfiguration() {
        this.logger.info(this.mqttConfiguration.toString());
    }

}