package Common;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.CRC32C;

public class RecordBatch {
    private long baseOffset;

    private int batchLength;

    private int partitionLeaderEpoch;

    private byte magicByte;

    private byte[] crc;

    private short attributes;

    private int lastOffsetDelta;

    private long baseTimestamp;

    private long maxTimestamp;

    private long producerId;

    private short producerEpoch;

    private int baseSequence;

    private int recordLength;

    private List<Record> records;

    public void response(DataOutputStream dataOutputStream)
    {
        try {
            dataOutputStream.writeLong(baseOffset);
            dataOutputStream.writeInt(batchLength);
            System.out.println("Batch length: " + batchLength);
            dataOutputStream.writeInt(partitionLeaderEpoch);
            dataOutputStream.writeByte(magicByte);

            ByteArrayOutputStream payloadStream = new ByteArrayOutputStream();
            DataOutputStream tempDos = new DataOutputStream(payloadStream);

            tempDos.writeShort(attributes);
            tempDos.writeInt(lastOffsetDelta);
            tempDos.writeLong(baseTimestamp);
            tempDos.writeLong(maxTimestamp);
            tempDos.writeLong(producerId);
            tempDos.writeShort(producerEpoch);
            tempDos.writeInt(baseSequence);
            tempDos.writeInt(recordLength);
            for (Record record : records) {
                record.response(tempDos);
            }
            
            byte[] payloadBytes = payloadStream.toByteArray();

            CRC32C crc32c = new CRC32C();
            crc32c.update(payloadBytes);
            int crcValue = (int) crc32c.getValue();

            dataOutputStream.writeInt(crcValue);

            dataOutputStream.write(payloadBytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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

    public byte[] getCrc() {
        return crc;
    }

    public void setCrc(byte[] crc) {
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
