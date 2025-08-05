package Fetch;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class FetchResponse {
    private int throttleTimeMs;

    private short errorCode;

    private int sessionId;

    private int topicLength;

    private List<TopicResponse> topicResponses;

    private byte tagBuffer;

    public void response(DataOutputStream dataOutputStream)
    {
        try {
            dataOutputStream.writeInt(throttleTimeMs);
            dataOutputStream.writeShort(errorCode);
            dataOutputStream.writeInt(sessionId);
            dataOutputStream.write(topicLength);
            for (TopicResponse topicResponse : this.topicResponses) {
                topicResponse.response(dataOutputStream);
            }
            // Tag buffer
            dataOutputStream.writeByte(tagBuffer);
        } catch (IOException e) {
        }
    }

    public int getThrottleTimeMs() {
        return throttleTimeMs;
    }

    public void setThrottleTimeMs(int throttleTimeMs) {
        this.throttleTimeMs = throttleTimeMs;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
    }

    public int getSessionId() {
        return sessionId;
    }

    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }

    public int getTopicLength() {
        return topicLength;
    }

    public void setTopicLength(int topicLength) {
        this.topicLength = topicLength;
    }

    public List<TopicResponse> getTopicResponses() {
        return topicResponses;
    }

    public void setTopicResponses(List<TopicResponse> topicResponses) {
        this.topicResponses = topicResponses;
    }

    public byte getTagBuffer() {
        return tagBuffer;
    }

    public void setTagBuffer(byte tagBuffer) {
        this.tagBuffer = tagBuffer;
    }
}
