package org.kbit.flume;

import com.influxdb.client.*;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import java.time.Instant;
import java.util.Base64;

public class InfluxDBSink extends AbstractSink implements Configurable {

    private InfluxDBClient influxDBClient;
    private WriteApi writeApi;
    private String measurement;
    private String tagKey;
    private String tagValue;

    public InfluxDBSink() {
    }

    @Override
    public void configure(Context context) {
        String url = context.getString("url");
        char[] token = context.getString("token").toCharArray();
        String org = context.getString("org");
        String bucket = context.getString("bucket");

        // Create the InfluxDB client
        this.influxDBClient = InfluxDBClientFactory.create(url, token, org, bucket);

        this.writeApi = influxDBClient.makeWriteApi();

        this.measurement = context.getString("measurement");
        this.tagKey = context.getString("tagKey");
        this.tagValue = context.getString("tagValue");
    }

    @Override
    public Sink.Status process() throws EventDeliveryException {
        Sink.Status result = Status.READY;
        Channel channel = this.getChannel();
        Transaction transaction = channel.getTransaction();
        Event event = null;

        try {
            transaction.begin();
            event = channel.take();
            if (event != null) {
                DynamicInfluxDBWriter writer =
                        new DynamicInfluxDBWriter(
                                this.measurement,
                                this.tagKey,
                                this.tagValue);

                String bodyBase64 = Base64.getEncoder().encodeToString(event.getBody());
                Point point = writer.createPoint(bodyBase64);
                writeApi.writePoint(point);

            } else {
                result = Status.BACKOFF;
            }
            transaction.commit();
        } catch (Exception var9) {
            transaction.rollback();
            throw new EventDeliveryException("Failed to log event: " + event, var9);
        } finally {
            transaction.close();
        }
        return result;
    }

    public static class DynamicInfluxDBWriter {
        private final String measurementName;
        private final String tagKey;
        private final String tagValue;

        public DynamicInfluxDBWriter(String measurementName, String tagKey, String tagValue) {
            this.measurementName = measurementName;
            this.tagKey = tagKey;
            this.tagValue = tagValue;
        }

        public Point createPoint(String value) {
            return createPoint(value, null);
        }

        public Point createPoint(String value, Instant time) {
            Point point = Point.measurement(measurementName)
                    .addTag(tagKey, tagValue)
                    .addField("value", value);

            if (time != null) {
                point.time(time, WritePrecision.NS);
            } else {
                point.time(Instant.now(), WritePrecision.NS);
            }

            return point;
        }
    }

    @Override
    public synchronized void start() {
        super.start();
    }

    @Override
    public synchronized void stop() {
        this.influxDBClient.close();
        super.stop();
    }
}