package Fetch;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class TopicResponse {
    private byte[] topicUUID;

    private int paritionLength;

    private List<PartitionResponse> partitionResponses;

    private byte tagBuffer;

    public void response(DataOutputStream dataOutputStream)
    {
        try {
            dataOutputStream.write(topicUUID);
            dataOutputStream.write(paritionLength);
            for (PartitionResponse partitionResponse : this.partitionResponses) {
                partitionResponse.response(dataOutputStream);
            }
            dataOutputStream.writeByte(tagBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setTopicUUID(byte[] topicUUID) {
        this.topicUUID = topicUUID;
    }

    public void setParitionLength(int paritionLength) {
        this.paritionLength = paritionLength;
    }

    public void setPartitionResponses(List<PartitionResponse> partitionResponses) {
        this.partitionResponses = partitionResponses;
    }
}
