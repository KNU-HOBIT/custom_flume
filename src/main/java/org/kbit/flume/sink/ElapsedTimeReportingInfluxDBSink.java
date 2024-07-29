package org.kbit.flume.sink;

import com.influxdb.client.write.events.WriteSuccessEvent;
import org.apache.flume.*;
import org.kbit.flume.conf.KafkaProdConfig;
import org.kbit.flume.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.Context;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

public class ElapsedTimeReportingInfluxDBSink extends InfluxDBSink {
    /**
     * This class extends InfluxDBSink and is responsible for measuring and reporting the elapsed time
     * between different stages of event processing in a Flume agent.
     *<p>
     * Key Responsibilities:
     * 1. Source to Sink Elapsed Time: Measures and reports the elapsed time from when an event is created
     *    at the source to when it is processed at the sink. This elapsed time data is formatted as
     *    "<messageID>:<Source - Sink elapsedTime>" and sent to the Kafka topic specified by 'kafka.topic1'.
     *</p>
     * 2. DB Write to Success Callback Elapsed Time: Measures and reports the elapsed time from when an
     *    event is written to the database to when the write success callback is invoked. This elapsed time
     *    data is formatted as "<messageID>:<DB write - Success Callback elapsedTime>" and sent to the
     *    Kafka topic specified by 'kafka.topic2'.
     *</p>
     * The class configures a Kafka producer for sending these latency metrics to the specified Kafka topics
     * and listens for write success events to report database write latencies.
     */
    private final Logger logger = LoggerFactory.getLogger(ElapsedTimeReportingInfluxDBSink.class);
    private KafkaProducer<String, String> kafkaProducer;
    private KafkaProdConfig kafkaProdConfig;
    private String topic1;
    private String topic2;

    @Override
    public void configure(Context context) {
        // Read and configure Kafka properties
        configureKafkaProducer(context);

        // Source - Sink Elapsed 딜레이용 토픽.
        this.topic1 = context.getString("kafka.topic1");

        // DB write - Success Callback Elapsed 딜레이용 토픽.
        this.topic2 = context.getString("kafka.topic2");

        super.configure(context); // url, token, org, bucket, measurement, tagKey, tagValue

        // Register Success Callback
        registerSuccessCallback();
    }


    @Override
    public synchronized void start() {
        this.logger.info(super.influxDBConfig.toString());

        logger.info(kafkaProdConfig.toString());

        Utils.logGitCommitLog(this.logger);

        super.start();
    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        txn.begin();

        try {
            Event event = channel.take();
            if (event != null) {

                // Source - Sink 사이의 Elapsed Time. 측정을 위한 Code.
                processElapsedTime(event);

                // DB Write하기 전에 시간 측정 다시 하고, ( 위 작업에 대한 시간은 배제하기위해. )
                // Write.(비동기, 즉, 내부적으로 Batch처리가 된다.)
                writeEventToDB(event);

                // 비동기로 쓰기때문에 Success콜백에서 시간 측정 후 딜레이 계산해서 전달.
                // 해당 부분 코드는 public void configure(Context context) 함수에 구현.

                txn.commit();
                return Status.READY;
            } else {
                txn.rollback();
                return Status.BACKOFF;
            }
        } catch (ChannelException e) {
            txn.rollback();
            throw new EventDeliveryException("Failed to process event", e);
        } finally {
            txn.close();
        }
    }


    /**
     * Processes the elapsed time for an event from source to sink.
     * <p>
     * This method calculates the time elapsed from when an event was created at the source
     * to when it is processed at the sink. It extracts the timestamp and message ID from the event headers,
     * calculates the elapsed time, and sends this information to a Kafka topic for visualization.
     *
     * @param event The event for which the elapsed time is being processed.<p>
     *              The event is expected to contain:<p>
     *              - A header with the key "timestamp" representing the creation time at the source in milliseconds.<p>
     *              - A header with the key "msgId" representing the unique message ID of the event.<p>
     */
    private void processElapsedTime(Event event) {
        long currentTimestamp = System.currentTimeMillis();
        String timestampStr = event.getHeaders().get("timestamp");
        String messageID = event.getHeaders().get("msgId");
        if (timestampStr != null) {
            long eventTimestamp = Long.parseLong(timestampStr);
            long elapsedTime = currentTimestamp - eventTimestamp;

            String delayMessage = messageID + ":" + elapsedTime;

            logger.debug("Elapsed time for Source's event: {} ms", elapsedTime);
            // 외부로 전송 코드. ( Source - Sink Elapsed Time 데이터를 모아, 시각화하기 위해서.)
            // "<messageID>:<Source - Sink elapsedTime>"형식의 데이터를 Kafka topic1으로 전송.
            sendToKafka(topic1, delayMessage);
        } else {
            logger.warn("Event does not contain timestamp header");
        }
    }



    private void sendToKafka(String topic, String message) {
        kafkaProducer.send(new ProducerRecord<>(topic, message), (metadata, exception) -> {
            if (exception != null) {
                logger.error("Failed to send message to Kafka", exception);
            } else {
                logger.debug("Sent message to Kafka: " + message);
            }
        });
    }




    private void configureKafkaProducer(Context context) {
        // Read Kafka configuration properties
        String bootstrapServers = context.getString("kafka.bootstrap.servers");

        // Read additional Kafka producer properties
        Map<String, String> producerProps = new HashMap<>();
        for (Map.Entry<String, String> entry : context.getParameters().entrySet()) {
            if (entry.getKey().startsWith("kafka.prod.")) {
                String producerPropKey = entry.getKey().substring("kafka.prod.".length());
                producerProps.put(producerPropKey, entry.getValue());
            }
        }

        // Create KafkaConfig object
        this.kafkaProdConfig = new KafkaProdConfig(bootstrapServers, producerProps);

        // Initialize Kafka producer
        this.kafkaProducer = new KafkaProducer<>(kafkaProdConfig.toProperties());
    }


    private void registerSuccessCallback() {
        super.writeApi.listenEvents(WriteSuccessEvent.class, (event) -> {
            // 이벤트 발생 시각 (밀리초 단위)
            long eventTime = System.currentTimeMillis();

            logger.info("WriteSuccessEvent: " + event);

            // 라인 프로토콜 데이터를 개별 라인으로 분리
            String[] lines = event.getLineProtocol().split("\n");

            for (String line : lines) {
                // 각 라인에서 타임스탬프 추출
                String[] parts = line.split(" ");
                if (parts.length == 3) {
                    long pointTime = Long.parseLong(parts[2]) / 1000000; // 나노초를 밀리초로 변환

                    // 타임스탬프 차이 계산
                    long timeDifference = eventTime - pointTime;

                    // msgId 추출
                    String messageID = Utils.extractFieldValue(line, "msgId");

                    // 딜레이 전송.
                    String delayMessage = messageID + ":" + timeDifference;

                    // "<messageID>:<DB write - Success Callback elapsedTime>"형식의 데이터를 topic2으로 전송.
                    sendToKafka(topic2, delayMessage);
                }
            }
        });
    }


    @Override
    public synchronized void stop() {
        super.stop();
    }
}
