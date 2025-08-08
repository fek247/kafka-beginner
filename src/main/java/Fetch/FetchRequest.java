package Fetch;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import Helpers.VarIntReader;

public class FetchRequest {
    private int maxWaitMs;

    private int minBytes;

    private int maxBytes;

    private byte isolationLevel;

    private int sessionId;

    private int sessionEpoch;

    private int topicLength;

    private List<TopicRequest> topicRequests;

    private int forgottenTopicLength;

    private List<ForgottenTopicRequest> forgottenTopicRequests;

    private int rackIdLength;

    private byte[] rackIdContent;

    private byte tagBuffer;

    public void request(DataInputStream dataInputStream) {
        try {
            setMaxWaitMs(dataInputStream.readInt());
            setMinBytes(dataInputStream.readInt());
            setMaxBytes(dataInputStream.readInt());
            setIsolationLevel(dataInputStream.readByte());
            setSessionId(dataInputStream.readInt());
            setSessionEpoch(dataInputStream.readInt());
            int topicLength = VarIntReader.readUnsignedVarInt(dataInputStream);
            setTopicLength(topicLength);
            System.out.println("Topic length: " + topicLength);
            List<TopicRequest> topicRequests = new ArrayList<>();
            for (int i = 0; i < topicLength - 1; i++) {
                TopicRequest topicRequest = new TopicRequest();
                topicRequest.request(dataInputStream);
                topicRequests.add(topicRequest);
            }
            setTopicRequests(topicRequests);
            int forgottenTopicLength = VarIntReader.readUnsignedVarInt(dataInputStream);
            setForgottenTopicLength(forgottenTopicLength);
            for (int i = 0; i < forgottenTopicLength - 1; i++) {
                ForgottenTopicRequest forgottenTopicRequest = new ForgottenTopicRequest();
                forgottenTopicRequest.request(dataInputStream);
            }
            int rackIdLength = VarIntReader.readUnsignedVarInt(dataInputStream);
            setRackIdLength(rackIdLength);
            byte[] rackId = new byte[rackIdLength - 1];
            dataInputStream.read(rackId);
            setRackIdContent(rackId);
            setTagBuffer(dataInputStream.readByte());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getMaxWaitMs() {
        return maxWaitMs;
    }

    public void setMaxWaitMs(int maxWaitMs) {
        this.maxWaitMs = maxWaitMs;
    }

    public int getMinBytes() {
        return minBytes;
    }

    public void setMinBytes(int minBytes) {
        this.minBytes = minBytes;
    }

    public int getMaxBytes() {
        return maxBytes;
    }

    public void setMaxBytes(int maxBytes) {
        this.maxBytes = maxBytes;
    }

    public byte getIsolationLevel() {
        return isolationLevel;
    }

    public void setIsolationLevel(byte isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public int getSessionId() {
        return sessionId;
    }

    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }

    public int getSessionEpoch() {
        return sessionEpoch;
    }

    public void setSessionEpoch(int sessionEpoch) {
        this.sessionEpoch = sessionEpoch;
    }

    public List<TopicRequest> getTopicRequests() {
        return topicRequests;
    }

    public void setTopicRequests(List<TopicRequest> topicRequests) {
        this.topicRequests = topicRequests;
    }

    public List<ForgottenTopicRequest> getForgottenTopicRequests() {
        return forgottenTopicRequests;
    }

    public void setForgottenTopicRequests(List<ForgottenTopicRequest> forgottenTopicRequests) {
        this.forgottenTopicRequests = forgottenTopicRequests;
    }

    public int getRackIdLength() {
        return rackIdLength;
    }

    public void setRackIdLength(int rackIdLength) {
        this.rackIdLength = rackIdLength;
    }

    public byte[] getRackIdContent() {
        return rackIdContent;
    }

    public void setRackIdContent(byte[] rackIdContent) {
        this.rackIdContent = rackIdContent;
    }

    public int getTopicLength() {
        return topicLength;
    }

    public void setTopicLength(int topicLength) {
        this.topicLength = topicLength;
    }

    public int getForgottenTopicLength() {
        return forgottenTopicLength;
    }

    public void setForgottenTopicLength(int forgottenTopicLength) {
        this.forgottenTopicLength = forgottenTopicLength;
    }

    public byte getTagBuffer() {
        return tagBuffer;
    }

    public void setTagBuffer(byte tagBuffer) {
        this.tagBuffer = tagBuffer;
    }
}
