package TopicPartition;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class TopicResponse {
    private short errorCode;

    private int topicNameLength;

    private byte[] topicName;

    private byte[] topicUUID;

    private boolean isInternal;

    private int partitionsLength;

    private List<PartitionResponse> partitionsResponse;

    private int topicAuthorizedOperations;

    private byte tagBuffer;

    private byte cursor;

    public void response(DataOutputStream dataOutputStream)
    {
        try {
            dataOutputStream.writeShort(errorCode);
            dataOutputStream.write(topicNameLength);
            dataOutputStream.write(topicName);
            dataOutputStream.write(topicUUID);
            dataOutputStream.writeBoolean(isInternal);
            dataOutputStream.write(partitionsLength);
            for (PartitionResponse partitionResponse : partitionsResponse) {
                partitionResponse.response(dataOutputStream);
            }
            dataOutputStream.writeInt(3576);
            dataOutputStream.writeByte(tagBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public short getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
    }

    public int getTopicNameLength() {
        return topicNameLength;
    }

    public void setTopicNameLength(int topicNameLength) {
        this.topicNameLength = topicNameLength;
    }

    public byte[] getTopicName() {
        return topicName;
    }

    public void setTopicName(byte[] topicName) {
        this.topicName = topicName;
    }

    public byte[] getTopicUUID() {
        return topicUUID;
    }

    public void setTopicUUID(byte[] topicUUID) {
        this.topicUUID = topicUUID;
    }

    public boolean isInternal() {
        return isInternal;
    }

    public void setInternal(boolean isInternal) {
        this.isInternal = isInternal;
    }

    public int getPartitionsLength() {
        return partitionsLength;
    }

    public void setPartitionsLength(int partitionsLength) {
        this.partitionsLength = partitionsLength;
    }

    public List<PartitionResponse> getPartitionsResponse() {
        return partitionsResponse;
    }

    public void setPartitionsResponse(List<PartitionResponse> partitionsResponse) {
        this.partitionsResponse = partitionsResponse;
    }

    public int getTopicAuthorizedOperations() {
        return topicAuthorizedOperations;
    }

    public void setTopicAuthorizedOperations(int topicAuthorizedOperations) {
        this.topicAuthorizedOperations = topicAuthorizedOperations;
    }

    public byte getTagBuffer() {
        return tagBuffer;
    }

    public void setTagBuffer(byte tagBuffer) {
        this.tagBuffer = tagBuffer;
    }

    public byte getCursor() {
        return cursor;
    }

    public void setCursor(byte cursor) {
        this.cursor = cursor;
    }
}
