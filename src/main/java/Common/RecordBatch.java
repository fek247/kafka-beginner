package Common;

import java.util.List;

public class RecordBatch {
    private long baseOffset;

    private int batchLength;

    private int partitionLeaderEpoch;

    private byte magicByte;

    private int crc;

    private short attributes;

    private int lastOffsetDelta;

    private long baseTimestamp;

    private long maxTimestamp;

    private long producerId;

    private short producerEpoch;

    private int baseSequence;

    private int recordLength;

    private List<Record> records;

    public long getBaseOffset() {
        return baseOffset;
    }

    public void setBaseOffset(long baseOffset) {
        this.baseOffset = baseOffset;
    }

    public int getBatchLength() {
        return batchLength;
    }

    public void setBatchLength(int batchLength) {
        this.batchLength = batchLength;
    }

    public int getPartitionLeaderEpoch() {
        return partitionLeaderEpoch;
    }

    public void setPartitionLeaderEpoch(int partitionLeaderEpoch) {
        this.partitionLeaderEpoch = partitionLeaderEpoch;
    }

    public byte getMagicByte() {
        return magicByte;
    }

    public void setMagicByte(byte magicByte) {
        this.magicByte = magicByte;
    }

    public int getCrc() {
        return crc;
    }

    public void setCrc(int crc) {
        this.crc = crc;
    }

    public short getAttributes() {
        return attributes;
    }

    public void setAttributes(short attributes) {
        this.attributes = attributes;
    }

    public int getLastOffsetDelta() {
        return lastOffsetDelta;
    }

    public void setLastOffsetDelta(int lastOffsetDelta) {
        this.lastOffsetDelta = lastOffsetDelta;
    }

    public long getBaseTimestamp() {
        return baseTimestamp;
    }

    public void setBaseTimestamp(long baseTimestamp) {
        this.baseTimestamp = baseTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public void setMaxTimestamp(long maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }

    public long getProducerId() {
        return producerId;
    }

    public void setProducerId(long producerId) {
        this.producerId = producerId;
    }

    public short getProducerEpoch() {
        return producerEpoch;
    }

    public void setProducerEpoch(short producerEpoch) {
        this.producerEpoch = producerEpoch;
    }

    public int getBaseSequence() {
        return baseSequence;
    }

    public void setBaseSequence(int baseSequence) {
        this.baseSequence = baseSequence;
    }

    public int getRecordLength() {
        return recordLength;
    }

    public void setRecordLength(int recordLength) {
        this.recordLength = recordLength;
    }

    public List<Record> getRecords() {
        return records;
    }

    public void setRecords(List<Record> records) {
        this.records = records;
    }
}
