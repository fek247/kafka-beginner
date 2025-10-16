package Produce;

import java.io.DataOutputStream;
import java.io.IOException;

public class PartitionResponse {
    private int partitionId;

    private short errorCode;

    private long baseOffset;

    private long logAppendTime;

    private long logStartOffset;

    private byte recordErrorLength;

    private byte errorMessage;

    private byte tagBuffer;

    public void response(DataOutputStream dataOutputStream)
    {
        try {
            dataOutputStream.writeInt(partitionId);
            dataOutputStream.writeShort(errorCode);
            dataOutputStream.writeLong(baseOffset);
            dataOutputStream.writeLong(logAppendTime);
            dataOutputStream.writeLong(logStartOffset);
            dataOutputStream.writeByte(recordErrorLength);
            dataOutputStream.writeByte(errorMessage);
            dataOutputStream.writeByte(tagBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
    }

    public void setBaseOffset(long baseOffset) {
        this.baseOffset = baseOffset;
    }

    public void setLogAppendTime(long logAppendTime) {
        this.logAppendTime = logAppendTime;
    }

    public void setLogStartOffset(long logStartOffset) {
        this.logStartOffset = logStartOffset;
    }

    public void setRecordErrorLength(byte recordErrorLength) {
        this.recordErrorLength = recordErrorLength;
    }

    public void setErrorMessage(byte errorMessage) {
        this.errorMessage = errorMessage;
    }

    public void setTagBuffer(byte tagBuffer) {
        this.tagBuffer = tagBuffer;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public long getLogAppendTime() {
        return logAppendTime;
    }

    public long getLogStartOffset() {
        return logStartOffset;
    }

    public byte getRecordErrorLength() {
        return recordErrorLength;
    }

    public byte getErrorMessage() {
        return errorMessage;
    }

    public byte getTagBuffer() {
        return tagBuffer;
    }

    
}
