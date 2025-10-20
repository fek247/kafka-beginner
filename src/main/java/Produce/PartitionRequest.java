package Produce;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import Common.RecordBatch;
import Helpers.VarIntReader;

public class PartitionRequest {
    private int partitionIndex;

    private int recordBatchSize;

    private List<RecordBatch> recordBatchs;

    private byte tagBuffer;

    public void request(DataInputStream dataInputStream)
    {
        try {
            setPartitionIndex(dataInputStream.readInt());
            setRecordBatchSize(VarIntReader.readUnsignedVarInt(dataInputStream));
            System.out.println("Record batch size: " + recordBatchSize);
            recordBatchs = new ArrayList<>();
            for (int i = 0; i < recordBatchSize - 1; i++) {
                RecordBatch recordBatch = new RecordBatch();
                recordBatch.request(dataInputStream);
                recordBatchs.add(recordBatch);
            }
            setRecordBatchs(recordBatchs);
            setTagBuffer(dataInputStream.readByte());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    public void setPartitionIndex(int partitionIndex) {
        this.partitionIndex = partitionIndex;
    }

    public int getRecordBatchSize() {
        return recordBatchSize;
    }

    public void setRecordBatchSize(int recordBatchSize) {
        this.recordBatchSize = recordBatchSize;
    }

    public byte getTagBuffer() {
        return tagBuffer;
    }

    public void setTagBuffer(byte tagBuffer) {
        this.tagBuffer = tagBuffer;
    }

    public List<RecordBatch> getRecordBatchs() {
        return recordBatchs;
    }

    public void setRecordBatchs(List<RecordBatch> recordBatchs) {
        this.recordBatchs = recordBatchs;
    }

    
}
