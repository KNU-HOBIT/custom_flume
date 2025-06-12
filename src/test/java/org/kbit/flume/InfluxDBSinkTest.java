
package org.kbit.flume;

import org.apache.flume.*;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.Before;
import org.junit.Test;
import org.kbit.flume.sink.InfluxDBSink;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class InfluxDBSinkTest {

    private InfluxDBSink sink;
    private Context context;
    private Channel channel;


    private static final Properties properties = new Properties();

    static {
        try (InputStream input = InfluxDBSinkTest.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find config.properties");
            }
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Error loading config.properties", e);
        }
    }

    @Before
    public void setUp() {
        sink = new InfluxDBSink();
        context = new Context();
        channel = new MemoryChannel();

        // InfluxDB 연결 정보 설정 - properties에서 읽어서 context에 넣기
        context.put("url", properties.getProperty("InfluxDBSinkTest.url"));
        context.put("token", properties.getProperty("InfluxDBSinkTest.token"));
        context.put("org", properties.getProperty("InfluxDBSinkTest.org"));
        context.put("bucket", properties.getProperty("InfluxDBSinkTest.bucket"));
        context.put("measurement", properties.getProperty("InfluxDBSinkTest.measurement"));
        context.put("tagKey", properties.getProperty("InfluxDBSinkTest.tagKey"));
        context.put("tagValue", properties.getProperty("InfluxDBSinkTest.tagValue"));
    }

    @Test
    public void testConfigure() {
        try {
            sink.configure(context);
        } catch (Exception e) {
            fail("Exception should not be thrown: " + e.getMessage());
        }
    }

    @Test
    public void testProcess() throws EventDeliveryException {
        // Given
        Configurables.configure(channel, new Context());
        sink.configure(context);
        sink.setChannel(channel);

        // Prepare test event
        Event event = EventBuilder.withBody("test event body".getBytes());
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        channel.put(event);
        transaction.commit();
        transaction.close();

        // When
        Sink.Status status = sink.process();

        // Then
        assertEquals(Sink.Status.READY, status);
    }
}
