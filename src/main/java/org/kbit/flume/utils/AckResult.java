package org.kbit.flume.utils;

public class AckResult {
    private final String messageID;
    private final byte[] messageContent;

    public AckResult(String messageId, byte[] messageContent) {
        this.messageID = messageId;
        this.messageContent = messageContent;
    }

    public String getMessageID() {
        return messageID;
    }

    public byte[] getMessageContent() {
        return messageContent;
    }
}