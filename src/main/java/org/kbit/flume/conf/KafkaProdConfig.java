package org.kbit.flume.conf;

import java.util.Map;
import java.util.Properties;

public class KafkaProdConfig {
    private final String bootstrapServers;
    private final Map<String, String> ProducerProps;

    public KafkaProdConfig(String bootstrapServers, Map<String, String> ProducerProps) {
        this.bootstrapServers = bootstrapServers;
        this.ProducerProps = ProducerProps;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }


    public Properties toProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.putAll(ProducerProps);
        return props;
    }
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("KafkaProdConfig configured with:\n")
                .append("Bootstrap Servers: ").append(getBootstrapServers()).append("\n");

        if (ProducerProps != null && !ProducerProps.isEmpty()) {
            sb.append("Additional Properties:\n");
            for (Map.Entry<String, String> entry : ProducerProps.entrySet()) {
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
        } else {
            sb.append("No additional properties configured.\n");
        }

        return sb.toString();
    }
}
