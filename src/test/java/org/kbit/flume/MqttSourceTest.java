package org.kbit.flume;

import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.eclipse.paho.client.mqttv3.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.kbit.flume.source.MqttSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.InputStream;


import static org.junit.jupiter.api.Assertions.*;
import static org.awaitility.Awaitility.*;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MqttSourceTest {

    private static final Logger logger = LoggerFactory.getLogger(MqttSourceTest.class);

    private MqttSource source;
    private Channel channel;
    private MqttClient testClient;
    private ExecutorService executorService;

    // Configuration constants - loaded from config.properties
    private static final String BROKER_URL;
    private static final String TOPIC;
    private static final String USERNAME;
    private static final String PASSWORD;
    private static final int THREAD_POOL_SIZE;
    private static final Duration DEFAULT_TIMEOUT;
    private static final Duration MESSAGE_PROCESSING_TIMEOUT;

    static {
        Properties properties = new Properties();
        try (InputStream input = MqttSourceTest.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find config.properties");
            }
            properties.load(input);

            // Initialize constants from properties
            BROKER_URL = properties.getProperty("MqttSourceTest.broker.url");
            TOPIC = properties.getProperty("MqttSourceTest.topic.prefix") + System.currentTimeMillis();
            USERNAME = properties.getProperty("MqttSourceTest.username");
            PASSWORD = properties.getProperty("MqttSourceTest.password");
            THREAD_POOL_SIZE = Integer.parseInt(properties.getProperty("MqttSourceTest.thread.pool.size"));
            DEFAULT_TIMEOUT = Duration.ofSeconds(Long.parseLong(properties.getProperty("MqttSourceTest.default.timeout.seconds")));
            MESSAGE_PROCESSING_TIMEOUT = Duration.ofSeconds(Long.parseLong(properties.getProperty("MqttSourceTest.message.processing.timeout.seconds")));

        } catch (IOException e) {
            throw new RuntimeException("Error loading config.properties", e);
        }
    }
    @BeforeEach
    public void setup() throws Exception {
        logger.info("Setting up MqttSourceTest...");

        // Initialize executor service
        executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        // Setup source
        source = new MqttSource();
        setupSource();

        // Setup channel
        channel = new MemoryChannel();
        setupChannel();

        // Connect source and channel
        connectSourceToChannel();

        // Start source
        source.start();

        // Wait a bit for source to fully start
        Thread.sleep(1000);

        // Setup test MQTT client
        setupTestClient();

        logger.info("MqttSourceTest setup completed successfully");
    }

    private void setupSource() {
        Context sourceContext = new Context();
        sourceContext.put("brokerUrl", BROKER_URL);
        sourceContext.put("topic", TOPIC);
        sourceContext.put("username", USERNAME);
        sourceContext.put("password", PASSWORD);
        sourceContext.put("threadPoolSize", String.valueOf(THREAD_POOL_SIZE));
        sourceContext.put("keepAliveInterval", "60");
        sourceContext.put("connectionTimeout", "30");
        sourceContext.put("cleanSession", "true");
        sourceContext.put("qos", "1");

        Configurables.configure(source, sourceContext);
        logger.debug("MqttSource configured successfully");
    }

    private void setupChannel() {
        Context channelContext = new Context();
        channelContext.put("capacity", "10000");
        channelContext.put("transactionCapacity", "1000");

        Configurables.configure(channel, channelContext);
        logger.debug("Channel configured successfully");
    }

    private void connectSourceToChannel() {
        List<Channel> channels = new ArrayList<>();
        channels.add(channel);

        ChannelSelector selector = new ReplicatingChannelSelector();
        selector.setChannels(channels);

        ChannelProcessor channelProcessor = new ChannelProcessor(selector);
        source.setChannelProcessor(channelProcessor);

        logger.debug("Source connected to channel successfully");
    }

    private void setupTestClient() throws MqttException {
        String clientId = "test-client-" + System.currentTimeMillis();
        testClient = new MqttClient(BROKER_URL, clientId);

        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName(USERNAME);
        options.setPassword(PASSWORD.toCharArray());
        options.setKeepAliveInterval(60);
        options.setConnectionTimeout(30);
        options.setCleanSession(true);

        testClient.connect(options);
        logger.debug("Test MQTT client connected with ID: {}", clientId);
    }

    @AfterEach
    public void teardown() {
        logger.info("Tearing down MqttSourceTest...");

        // Stop source
        if (source != null) {
            try {
                source.stop();
                logger.debug("MqttSource stopped");
            } catch (Exception e) {
                logger.warn("Error stopping MqttSource", e);
            }
        }

        // Disconnect test client
        if (testClient != null && testClient.isConnected()) {
            try {
                testClient.disconnect();
                logger.debug("Test MQTT client disconnected");
            } catch (MqttException e) {
                logger.warn("Error disconnecting test client", e);
            }
        }

        // Shutdown executor service
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Clear any remaining messages in channel
        clearChannel();

        logger.info("MqttSourceTest teardown completed");
    }

    private void clearChannel() {
        try {
            while (true) {
                Transaction transaction = channel.getTransaction();
                transaction.begin();
                try {
                    Event event = channel.take();
                    if (event == null) {
                        transaction.rollback();
                        break;
                    }
                    transaction.commit();
                } catch (Exception e) {
                    transaction.rollback();
                    break;
                } finally {
                    transaction.close();
                }
            }
        } catch (Exception e) {
            logger.debug("Error clearing channel", e);
        }
    }

    @Test
    @Order(1)
    @DisplayName("Basic MQTT message reception test")
    public void testBasicMqttMessageReceived() throws Exception {
        String testMessage = "Hello, MQTT! " + System.currentTimeMillis();

        // Publish message
        publishMessage(testMessage);

        // Wait for and verify message
        Event receivedEvent = waitForEvent();
        assertNotNull(receivedEvent, "Event should not be null");
        assertEquals(testMessage, new String(receivedEvent.getBody(), StandardCharsets.UTF_8),
                "Message content should match");

        logger.info("Basic MQTT message test passed");
    }

    @Test
    @Order(2)
    @DisplayName("Multiple messages test with delays")
    public void testMultipleMessages() throws Exception {
        int messageCount = 3; // Reduced count for reliability
        List<String> testMessages = new ArrayList<>();

        // Prepare test messages with unique content
        for (int i = 0; i < messageCount; i++) {
            testMessages.add("Test message " + i + " - " + System.currentTimeMillis() + " - " + UUID.randomUUID());
        }

        // Publish all messages with delays to avoid race conditions
        for (String message : testMessages) {
            publishMessage(message);
            Thread.sleep(200); // Increased delay between messages
        }

        // Wait a bit more for all messages to be processed
        Thread.sleep(1000);

        // Verify all messages received
        Set<String> receivedMessages = new HashSet<>();
        for (int i = 0; i < messageCount; i++) {
            Event event = waitForEvent();
            assertNotNull(event, "Event " + i + " should not be null");
            String receivedContent = new String(event.getBody(), StandardCharsets.UTF_8);
            receivedMessages.add(receivedContent);
            logger.debug("Received message {}: {}", i, receivedContent);
        }

        assertEquals(messageCount, receivedMessages.size(), "All messages should be received");
        for (String originalMessage : testMessages) {
            assertTrue(receivedMessages.contains(originalMessage),
                    "Original message should be in received messages: " + originalMessage);
        }

        logger.info("Multiple messages test passed with {} messages", messageCount);
    }

    @Test
    @Order(3)
    @DisplayName("Large message test")
    public void testLargeMessage() throws Exception {
        // Create a large message (1KB)
        StringBuilder largeMessageBuilder = new StringBuilder();
        String uniqueId = UUID.randomUUID().toString();
        for (int i = 0; i < 50; i++) {
            largeMessageBuilder.append("Large message content ").append(i).append(" ").append(uniqueId).append(" ");
        }
        String largeMessage = largeMessageBuilder.toString();

        // Publish large message
        publishMessage(largeMessage);

        // Verify large message
        Event receivedEvent = waitForEvent();
        assertNotNull(receivedEvent, "Large message event should not be null");
        assertEquals(largeMessage, new String(receivedEvent.getBody(), StandardCharsets.UTF_8),
                "Large message content should match");

        logger.info("Large message test passed with message size: {} bytes", largeMessage.length());
    }

    @Test
    @Order(4)
    @DisplayName("Sequential message publishing test")
    public void testSequentialMessagePublishing() throws Exception {
        int messageCount = 5;
        List<String> sentMessages = new ArrayList<>();

        // Publish messages sequentially with unique content
        for (int i = 0; i < messageCount; i++) {
            String message = "Sequential-Message-" + i + "-" + System.currentTimeMillis() + "-" + UUID.randomUUID();
            sentMessages.add(message);
            publishMessage(message);
            Thread.sleep(300); // Wait between messages
        }

        // Wait for all messages to be processed
        Thread.sleep(1000);

        // Verify all messages received
        List<String> receivedMessages = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            Event event = waitForEvent();
            assertNotNull(event, "Event " + i + " should not be null");
            String receivedContent = new String(event.getBody(), StandardCharsets.UTF_8);
            receivedMessages.add(receivedContent);
            logger.debug("Received sequential message {}: {}", i, receivedContent);
        }

        assertEquals(messageCount, receivedMessages.size(), "All sequential messages should be received");

        // Check that all sent messages were received
        Set<String> sentSet = new HashSet<>(sentMessages);
        Set<String> receivedSet = new HashSet<>(receivedMessages);
        assertEquals(sentSet, receivedSet, "All sent messages should be received");

        logger.info("Sequential publishing test passed with {} messages", messageCount);
    }

    @Test
    @Order(5)
    @DisplayName("JSON message test")
    public void testJsonMessage() throws Exception {
        String uniqueId = UUID.randomUUID().toString();
        String jsonMessage = "{\n" +
                "  \"id\": \"" + uniqueId + "\",\n" +
                "  \"timestamp\": " + System.currentTimeMillis() + ",\n" +
                "  \"sensor_id\": \"temp_001\",\n" +
                "  \"temperature\": 23.5,\n" +
                "  \"humidity\": 65.2,\n" +
                "  \"status\": \"active\"\n" +
                "}";

        // Publish JSON message
        publishMessage(jsonMessage);

        // Verify JSON message
        Event receivedEvent = waitForEvent();
        assertNotNull(receivedEvent, "JSON message event should not be null");
        assertEquals(jsonMessage, new String(receivedEvent.getBody(), StandardCharsets.UTF_8),
                "JSON message content should match");

        logger.info("JSON message test passed");
    }

    @Test
    @Order(6)
    @DisplayName("Message reliability test")
    public void testMessageReliability() throws Exception {
        int messageCount = 5;
        AtomicInteger messageCounter = new AtomicInteger(0);
        List<String> sentMessages = Collections.synchronizedList(new ArrayList<>());

        // Send messages with confirmation
        for (int i = 0; i < messageCount; i++) {
            String message = "Reliability-Test-" + i + "-" + System.currentTimeMillis();
            sentMessages.add(message);

            MqttMessage mqttMessage = new MqttMessage(message.getBytes(StandardCharsets.UTF_8));
            mqttMessage.setQos(1); // Ensure delivery

            testClient.publish(TOPIC, mqttMessage);
            logger.debug("Published reliable message {}: {}", i, message.substring(0, Math.min(30, message.length())));

            // Wait between messages
            Thread.sleep(300);
        }

        // Wait for processing
        Thread.sleep(1500);

        // Collect all received messages
        List<String> receivedMessages = new ArrayList<>();
        int attempts = 0;
        while (receivedMessages.size() < messageCount && attempts < messageCount * 2) {
            Event event = tryTakeEventFromChannelWithTimeout(Duration.ofSeconds(2));
            if (event != null) {
                String content = new String(event.getBody(), StandardCharsets.UTF_8);
                receivedMessages.add(content);
                logger.debug("Received reliable message {}: {}", receivedMessages.size(),
                        content.substring(0, Math.min(30, content.length())));
            }
            attempts++;
        }

        assertEquals(messageCount, receivedMessages.size(),
                "All reliable messages should be received. Sent: " + sentMessages.size() +
                        ", Received: " + receivedMessages.size());

        logger.info("Message reliability test passed with {} messages", messageCount);
    }

    // Helper methods

    private void publishMessage(String message) throws MqttException {
        MqttMessage mqttMessage = new MqttMessage(message.getBytes(StandardCharsets.UTF_8));
        mqttMessage.setQos(1);
        mqttMessage.setRetained(false);
        testClient.publish(TOPIC, mqttMessage);
        logger.debug("Published message: {}", message.substring(0, Math.min(50, message.length())));
    }

    private Event waitForEvent() throws Exception {
        return await()
                .atMost(MESSAGE_PROCESSING_TIMEOUT)
                .pollInterval(Duration.ofMillis(100))
                .until(this::tryTakeEventFromChannel, Objects::nonNull);
    }

    private Event tryTakeEventFromChannel() {
        return tryTakeEventFromChannelWithTimeout(Duration.ofMillis(50));
    }

    private Event tryTakeEventFromChannelWithTimeout(Duration timeout) {
        Transaction transaction = channel.getTransaction();
        transaction.begin();

        try {
            Event event = channel.take();
            if (event != null) {
                transaction.commit();
                return event;
            } else {
                transaction.rollback();
                return null;
            }
        } catch (Exception e) {
            transaction.rollback();
            logger.debug("Error taking event from channel", e);
            return null;
        } finally {
            transaction.close();
        }
    }

    @Test
    @Order(7)
    @DisplayName("No extra messages test")
    public void testNoExtraMessages() throws Exception {
        // Send one message
        String testMessage = "Single-Test-Message-" + System.currentTimeMillis();
        publishMessage(testMessage);

        // Get the one message
        Event event = waitForEvent();
        assertNotNull(event, "Should receive exactly one message");
        assertEquals(testMessage, new String(event.getBody(), StandardCharsets.UTF_8));

        // Verify no extra messages
        Event extraEvent = tryTakeEventFromChannelWithTimeout(Duration.ofSeconds(2));
        assertNull(extraEvent, "Should not receive any extra messages");

        logger.info("No extra messages test passed");
    }
}