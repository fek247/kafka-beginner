package Produce;

import java.io.DataInputStream;
import java.io.IOException;

import Common.RecordBatch;
import Helpers.VarIntReader;

public class PartitionRequest {
    private int partitionIndex;

    private int recordBatchSize;

    private RecordBatch recordBatch;

    private byte tagBuffer;

    public void request(DataInputStream dataInputStream)
    {
        try {
            setPartitionIndex(dataInputStream.readInt());
            setRecordBatchSize(VarIntReader.readUnsignedVarInt(dataInputStream));
            System.out.println("Record batch size: " + recordBatchSize);
            RecordBatch recordBatch = new RecordBatch();
            recordBatch.request(dataInputStream);
            setRecordBatch(recordBatch);
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

    public RecordBatch getRecordBatch() {
        return recordBatch;
    }

    public void setRecordBatch(RecordBatch recordBatch) {
        this.recordBatch = recordBatch;
    }

    public byte getTagBuffer() {
        return tagBuffer;
    }

    public void setTagBuffer(byte tagBuffer) {
        this.tagBuffer = tagBuffer;
    }

    
}
