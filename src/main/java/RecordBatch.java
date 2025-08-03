public class RecordBatch {
    private long baseOffset;

    private int batchLength;

    private Record[] records;

    public void setBaseOffset(long baseOffset)
    {
        this.baseOffset = baseOffset;
    }

    public void setBatchLength(int batchLength)
    {
        this.batchLength = batchLength;
    }

    public long getBaseOffset()
    {
        return this.baseOffset;
    }

    public int getBatchLength()
    {
        return this.batchLength;
    }

    public void setRecords(Record[] records) {
        this.records = records;
    }

    public Record[] getRecords() {
        return records;
    }
}