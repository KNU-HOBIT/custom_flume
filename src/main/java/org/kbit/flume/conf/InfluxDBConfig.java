package org.kbit.flume.conf;

public class InfluxDBConfig {
    private final String url;
    private final char[] token;
    private final String org;
    private final String bucket;
    private final String measurement;
    private final String tagKey;
    private final String tagValue;

    public InfluxDBConfig(String url, char[] token, String org, String bucket, String measurement, String tagKey, String tagValue) {
        this.url = url;
        this.token = token;
        this.org = org;
        this.bucket = bucket;
        this.measurement = measurement;
        this.tagKey = tagKey;
        this.tagValue = tagValue;
    }

    public String getUrl() {return url;}
    public char[] getToken() {return token;}
    public String getOrg() {return org;}
    public String getBucket() {return bucket;}
    public String getMeasurement() {return measurement;}
    public String getTagKey() {return tagKey;}
    public String getTagValue() {return tagValue;}

    @Override
    public String toString() {
        return "InfluxDBConfig configured with:\n" +
                "URL: " + getUrl() + "\n" +
                "Organization: " + getOrg() + "\n" +
                "Bucket: " + getBucket() + "\n" +
                "Measurement: " + getMeasurement() + "\n" +
                "Tag Key: " + getTagKey() + "\n" +
                "Tag Value: " + getTagValue() + "\n";
    }
}