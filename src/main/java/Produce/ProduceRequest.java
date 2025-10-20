package Produce;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import Helpers.VarIntReader;

public class ProduceRequest {
    private byte transactionId;

    private short acks;

    private int timeoutMs;

    private int topicLength;

    private List<TopicRequest> topicRequests;

    private byte tagBuffer;

    public void request(DataInputStream dataInputStream)
    {
        try {
            setTransactionId(dataInputStream.readByte());
            setAcks(dataInputStream.readShort());
            setTimeoutMs(dataInputStream.readInt());
            setTopicLength(VarIntReader.readUnsignedVarInt(dataInputStream));
            List<TopicRequest> topicRequests = new ArrayList<>();
            for (int i = 0; i < topicLength - 1; i++) {
                TopicRequest topicRequest = new TopicRequest();
                topicRequest.request(dataInputStream);
                topicRequests.add(topicRequest);
            }
            setTopicRequests(topicRequests);
            setTagBuffer(dataInputStream.readByte());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setTransactionId(byte transactionId) {
        this.transactionId = transactionId;
    }

    public void setAcks(short acks) {
        this.acks = acks;
    }

    public void setTimeoutMs(int timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public void setTopicLength(int topicLength) {
        this.topicLength = topicLength;
    }

    public void setTopicRequests(List<TopicRequest> topicRequests) {
        this.topicRequests = topicRequests;
    }

    public void setTagBuffer(byte tagBuffer) {
        this.tagBuffer = tagBuffer;
    }

    public byte getTransactionId() {
        return transactionId;
    }

    public short getAcks() {
        return acks;
    }

    public int getTimeoutMs() {
        return timeoutMs;
    }

    public int getTopicLength() {
        return topicLength;
    }

    public List<TopicRequest> getTopicRequests() {
        return topicRequests;
    }

    public byte getTagBuffer() {
        return tagBuffer;
    }
}
