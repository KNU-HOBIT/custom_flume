package org.kbit.flume.sink;

import org.kbit.flume.conf.InfluxDBConfig;
import org.kbit.flume.utils.DynamicInfluxDBWriter;
import org.kbit.flume.utils.Utils;
import com.influxdb.client.*;
import com.influxdb.client.write.Point;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class InfluxDBSink extends AbstractSink implements Configurable {
    private final Logger logger = LoggerFactory.getLogger(InfluxDBSink.class);
    protected InfluxDBClient influxDBClient;
    protected WriteApi writeApi;
    protected InfluxDBConfig influxDBConfig;
    protected List<String> timeDifferences = new ArrayList<>();

    public InfluxDBSink() {}

    @Override
    public void configure(Context context) {
        influxDBConfig = new InfluxDBConfig(
                context.getString("url"),
                context.getString("token").toCharArray(),
                context.getString("org"),
                context.getString("bucket"),
                context.getString("measurement"),
                context.getString("tagKey"),
                context.getString("tagValue")
        );

        // Create the InfluxDB client
        this.influxDBClient = InfluxDBClientFactory.create(
                influxDBConfig.getUrl(),
                influxDBConfig.getToken(),
                influxDBConfig.getOrg(),
                influxDBConfig.getBucket()
        );
        this.writeApi = influxDBClient.makeWriteApi();
    }

    @Override
    public synchronized void start() {
        super.start();

        logger.info(influxDBConfig.toString());

        Utils.logGitCommitLog(logger);
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
                // Sink까지 데이터 도착.
                writeEventToDB(event);

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

    void writeEventToDB(Event event) {
        long currentTimestamp = System.currentTimeMillis();

        DynamicInfluxDBWriter writer =
                new DynamicInfluxDBWriter(
                        this.influxDBConfig.getMeasurement(),
                        this.influxDBConfig.getTagKey(),
                        this.influxDBConfig.getTagValue(),
                        event.getHeaders().get("msgId"));

        String bodyBase64 = Base64.getEncoder().encodeToString(event.getBody());
        Point point = writer.createPoint(bodyBase64, Instant.ofEpochMilli(currentTimestamp));

        // Write.(비동기, 즉, 내부적으로 Batch처리가 된다.)
        writeApi.writePoint(point);
    }

    @Override
    public synchronized void stop() {
        this.influxDBClient.close();
        super.stop();
    }
}