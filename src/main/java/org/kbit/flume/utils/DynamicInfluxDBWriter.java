package org.kbit.flume.utils;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

import java.time.Instant;

public class DynamicInfluxDBWriter {
    private final String measurementName;
    private final String tagKey;
    private final String tagValue;
    private final String msgId;

    public DynamicInfluxDBWriter(String measurementName, String tagKey, String tagValue, String msgId) {
        this.measurementName = measurementName;
        this.tagKey = tagKey;
        this.tagValue = tagValue;
        this.msgId = msgId;
    }

    public Point createPoint(String value, Instant time) {
        Point point = Point.measurement(measurementName)
                .addTag(tagKey, tagValue)
                .addField("value", value);
        if (msgId != null){
            point.addField("msgId",msgId);
        }

        if (time != null) {
            point.time(time, WritePrecision.NS);
        } else {
            point.time(Instant.now(), WritePrecision.NS);
        }

        return point;
    }
}