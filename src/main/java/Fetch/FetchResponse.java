package Fetch;

import java.io.DataOutputStream;
import java.io.IOException;

public class FetchResponse {
    private int throttleTimeMs;

    private short errorCode;

    private int sessionId;

    private int topicLength;

    public void response(DataOutputStream dataOutputStream)
    {
        try {
            dataOutputStream.writeInt(0);
            dataOutputStream.writeShort(0);
            dataOutputStream.writeInt(0);
            dataOutputStream.write(topicLength);
            // Tag buffer
            dataOutputStream.writeByte(0);
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
}
