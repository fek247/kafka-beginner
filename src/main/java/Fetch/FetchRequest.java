package Fetch;
import java.io.DataInputStream;
import java.io.IOException;
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

    private List<ForgottenTopicRequest> forgottenTopicRequests;

    private int rackIdLength;

    private byte[] rackIdContent;

    public void request(DataInputStream dataInputStream) {
        try {
            setMaxWaitMs(dataInputStream.readInt());
            setMinBytes(dataInputStream.readInt());
            setMaxBytes(dataInputStream.readInt());
            setIsolationLevel(dataInputStream.readByte());
            setSessionId(dataInputStream.readInt());
            setSessionEpoch(dataInputStream.readInt());
            int topicLength = VarIntReader.readSignedVarInt(dataInputStream);
            setTopicLength(topicLength);
            for (int i = 0; i < topicLength; i++) {
                
            }
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
}
