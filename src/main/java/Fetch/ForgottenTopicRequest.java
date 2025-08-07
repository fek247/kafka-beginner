package Fetch;

import java.io.DataInputStream;
import java.io.IOException;

import Helpers.VarIntReader;

public class ForgottenTopicRequest {
    private byte[] topicUUID;

    private int partitionLength;

    private int[] partitions;

    private byte tagBuffer;

    public void request(DataInputStream dataInputStream)
    {
        try {
            byte[] topicUUID = new byte[16];
            dataInputStream.read(topicUUID);
            setTopicUUID(topicUUID);
            int partitionLength = VarIntReader.readUnsignedVarInt(dataInputStream);
            int[] partitions = new int[partitionLength];
            for (int i = 0; i < partitionLength; i++) {
                partitions[i] = dataInputStream.readInt();
            }
            setPartitions(partitions);
            setTagBuffer(dataInputStream.readByte());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] getTopicUUID() {
        return topicUUID;
    }

    public void setTopicUUID(byte[] topicUUID) {
        this.topicUUID = topicUUID;
    }

    public int[] getPartitions() {
        return partitions;
    }

    public void setPartitions(int[] partitions) {
        this.partitions = partitions;
    }

    public byte getTagBuffer() {
        return tagBuffer;
    }

    public void setTagBuffer(byte tagBuffer) {
        this.tagBuffer = tagBuffer;
    }

    public int getPartitionLength() {
        return partitionLength;
    }

    public void setPartitionLength(int partitionLength) {
        this.partitionLength = partitionLength;
    }
}
