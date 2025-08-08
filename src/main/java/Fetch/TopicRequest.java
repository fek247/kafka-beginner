package Fetch;

import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.List;

import Helpers.VarIntReader;

public class TopicRequest {
    private byte[] topicUUID;

    private int partitionLength;

    private List<PartitionRequest> partitionRequests;

    public void request(DataInputStream dataInputStream)
    {
        try {
            byte[] topicUUID = new byte[16];
            dataInputStream.read(topicUUID);
            setTopicUUID(topicUUID);
            int partitionLength = VarIntReader.readUnsignedVarInt(dataInputStream);
            setPartitionLength(partitionLength);
            List<PartitionRequest> partitionRequests = new ArrayList<>();
            System.out.println("partition length: " + partitionLength);
            for (int i = 0; i < partitionLength - 1; i++) {
                PartitionRequest partitionRequest = new PartitionRequest();
                partitionRequest.request(dataInputStream);
                partitionRequests.add(partitionRequest);
            }
            setPartitionRequests(partitionRequests);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public byte[] getTopicUUID() {
        return topicUUID;
    }

    public void setTopicUUID(byte[] topicUUID) {
        this.topicUUID = topicUUID;
    }

    public int getPartitionLength() {
        return partitionLength;
    }

    public void setPartitionLength(int partitionLength) {
        this.partitionLength = partitionLength;
    }

    public List<PartitionRequest> getPartitionRequests() {
        return partitionRequests;
    }

    public void setPartitionRequests(List<PartitionRequest> partitionRequests) {
        this.partitionRequests = partitionRequests;
    }
}
