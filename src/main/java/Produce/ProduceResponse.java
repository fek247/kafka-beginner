package Produce;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import Helpers.VarIntReader;

public class ProduceResponse {
    private int topicLength;

    private List<TopicResponse> topicResponses;

    private int throttleTime;

    private byte tagBuffer;

    public void response(DataOutputStream dataOutputStream)
    {
        try {
            dataOutputStream.write(VarIntReader.encodeUnsignedVarInt(topicLength));
            for (TopicResponse topicResponse : this.topicResponses) {
                topicResponse.response(dataOutputStream);
            }
            // Throttle Time
            dataOutputStream.writeInt(throttleTime);
            // // Tag buffer
            dataOutputStream.writeByte(tagBuffer);
        } catch (IOException e) {
        }
    }

    public void setTopicLength(int topicLength) {
        this.topicLength = topicLength;
    }

    public void setTopicResponses(List<TopicResponse> topicResponses) {
        this.topicResponses = topicResponses;
    }

    public void setThrottleTime(int throttleTime) {
        this.throttleTime = throttleTime;
    }

    public void setTagBuffer(byte tagBuffer) {
        this.tagBuffer = tagBuffer;
    }
}
