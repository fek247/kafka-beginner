package Produce;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class TopicResponse {
    private int nameLength;

    private byte[] name;

    private int partitionLength;

    private List<PartitionResponse> partitionResponses;

    private byte tagBuffer;
    
    public void response(DataOutputStream dataOutputStream)
    {
        try {
            dataOutputStream.write(nameLength);
            dataOutputStream.write(name);
            for (PartitionResponse partitionResponse : this.partitionResponses) {
                partitionResponse.response(dataOutputStream);
            }
            dataOutputStream.writeByte(tagBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getNameLength() {
        return nameLength;
    }

    public byte[] getName() {
        return name;
    }

    public int getPartitionLength() {
        return partitionLength;
    }

    public List<PartitionResponse> getPartitionResponses() {
        return partitionResponses;
    }

    public byte getTagBuffer() {
        return tagBuffer;
    }

    public void setNameLength(int nameLength) {
        this.nameLength = nameLength;
    }

    public void setName(byte[] name) {
        this.name = name;
    }

    public void setPartitionLength(int partitionLength) {
        this.partitionLength = partitionLength;
    }

    public void setPartitionResponses(List<PartitionResponse> partitionResponses) {
        this.partitionResponses = partitionResponses;
    }

    public void setTagBuffer(byte tagBuffer) {
        this.tagBuffer = tagBuffer;
    }

    
}
