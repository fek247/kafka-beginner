package Produce;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import Helpers.VarIntReader;

public class TopicRequest {
    private int nameLength;

    private byte[] name;

    private int partitionLength;

    private List<PartitionRequest> partitionRequests;

    private byte tagBuffer;

    public void request(DataInputStream dataInputStream)
    {
        try {
            setNameLength(VarIntReader.readUnsignedVarInt(dataInputStream));
            byte[] name = new byte[nameLength - 1];
            dataInputStream.read(name);
            setName(name);
            setPartitionLength(VarIntReader.readUnsignedVarInt(dataInputStream));
            List<PartitionRequest> partitionRequests = new ArrayList<>();
            for (int i = 0; i < partitionLength - 1; i++) {
                PartitionRequest partitionRequest = new PartitionRequest();
                partitionRequest.request(dataInputStream);
                partitionRequests.add(partitionRequest);
            }
            setPartitionRequests(partitionRequests);
            byte tagBuffer = dataInputStream.readByte();
            setTagBuffer(tagBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    public void setPartitionRequests(List<PartitionRequest> partitionRequests) {
        this.partitionRequests = partitionRequests;
    }

    public void setTagBuffer(byte tagBuffer) {
        this.tagBuffer = tagBuffer;
    }

    public byte[] getName() {
        return name;
    }

    public int getPartitionLength() {
        return partitionLength;
    }

    public List<PartitionRequest> getPartitionRequests() {
        return partitionRequests;
    }
}
