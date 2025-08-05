package Fetch;

import java.io.DataInputStream;
import java.io.IOException;

public class PartitionRequest {
    private int partitionId;

    private int leader;

    private long fetchOffset;

    private int lastFetchedOffset;

    private long logStartOffset;

    private int partitionMaxBytes;

    private byte tagBuffer;

    public void request(DataInputStream dataInputStream)
    {
        try {
            setPartitionId(dataInputStream.readInt());
            setLeader(dataInputStream.readInt());
            setFetchOffset(dataInputStream.readLong());
            setLastFetchedOffset(dataInputStream.readInt());
            setLogStartOffset(dataInputStream.readLong());
            setPartitionMaxBytes(dataInputStream.readInt());
            setTagBuffer(dataInputStream.readByte());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public int getLeader() {
        return leader;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public long getFetchOffset() {
        return fetchOffset;
    }

    public void setFetchOffset(long fetchOffset) {
        this.fetchOffset = fetchOffset;
    }

    public int getLastFetchedOffset() {
        return lastFetchedOffset;
    }

    public void setLastFetchedOffset(int lastFetchedOffset) {
        this.lastFetchedOffset = lastFetchedOffset;
    }

    public long getLogStartOffset() {
        return logStartOffset;
    }

    public void setLogStartOffset(long logStartOffset) {
        this.logStartOffset = logStartOffset;
    }

    public int getPartitionMaxBytes() {
        return partitionMaxBytes;
    }

    public void setPartitionMaxBytes(int partitionMaxBytes) {
        this.partitionMaxBytes = partitionMaxBytes;
    }

    public byte getTagBuffer() {
        return tagBuffer;
    }

    public void setTagBuffer(byte tagBuffer) {
        this.tagBuffer = tagBuffer;
    }
}
