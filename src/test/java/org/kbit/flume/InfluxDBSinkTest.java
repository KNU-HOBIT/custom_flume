
package org.kbit.flume;

import org.apache.flume.*;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.Before;
import org.junit.Test;
import org.kbit.flume.sink.InfluxDBSink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class InfluxDBSinkTest {

    private InfluxDBSink sink;
    private Context context;
    private Channel channel;

    @Before
    public void setUp() {
        sink = new InfluxDBSink();
        context = new Context();
        channel = new MemoryChannel();

        // InfluxDB 연결 정보 설정
        context.put("url", "http://155.230.35.213:32145");
        context.put("token", "auyiYKUAAhTJLVBv20i2yCwVwmUA5lGJ");
        context.put("org", "influxdata");
        context.put("bucket", "default");
        context.put("measurement", "test_measurement");
        context.put("tagKey", "test_tagKey");
        context.put("tagValue", "123123");
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