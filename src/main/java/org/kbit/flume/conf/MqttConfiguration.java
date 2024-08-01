package org.kbit.flume.conf;

public class MqttConfiguration implements Cloneable {
    private String brokerUrl;
    private String topic;
    private String username;
    private String password;
    private int keepAliveInterval;
    private int connectionTimeout;
    private boolean cleanSession;
    private int qos;
    private String returnTopic;

    // Getters and setters
    public String getBrokerUrl() {
        return brokerUrl;
    }

    public void setBrokerUrl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getKeepAliveInterval() {
        return keepAliveInterval;
    }

    public void setKeepAliveInterval(int keepAliveInterval) {
        this.keepAliveInterval = keepAliveInterval;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public String getReturnTopic() {
        return returnTopic;
    }

    public void setReturnTopic(String returnTopic) {
        this.returnTopic = returnTopic;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    // Method to get configuration value by key
    public String get(String key) {
        switch (key) {
            case "brokerUrl":
                return getBrokerUrl();
            case "topic":
                return getTopic();
            case "username":
                return getUsername();
            case "password":
                return getPassword();
            case "keepAliveInterval":
                return String.valueOf(getKeepAliveInterval());
            case "connectionTimeout":
                return String.valueOf(getConnectionTimeout());
            case "cleanSession":
                return String.valueOf(isCleanSession());
            case "qos":
                return String.valueOf(getQos());
            default:
                return null;
        }
    }
    // Method to log the configuration values in a formatted way
    @Override
    public String toString() {
        return "MqttConfiguration configured with:\n" +
                "Broker URL: " + getBrokerUrl() + "\n" +
                "Return Topic: " + getReturnTopic() + "\n" +
                "Topic: " + getTopic() + "\n" +
                "Username: " + getUsername() + "\n" +
                "Password: " + getPassword() + "\n" +
                "Keep Alive Interval: " + getKeepAliveInterval() + "\n" +
                "Connection Timeout: " + getConnectionTimeout() + "\n" +
                "Clean Session: " + isCleanSession() + "\n" +
                "QoS: " + getQos() + "\n";
    }

}
