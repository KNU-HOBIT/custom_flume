package org.kbit.flume.utils;

public class MessageTuple {
    private final String messageID;
    private final byte[] data;

    public MessageTuple(String messageID, byte[] data) {
        this.messageID = messageID;
        this.data = data;
    }

    public String getMessageID() {
        return messageID;
    }

    public byte[] getData() {
        return data;
    }
}