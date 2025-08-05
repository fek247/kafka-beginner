package Abstract;

public abstract class Record {
    protected byte length;

    protected RecordValue value;

    public void setLength(byte length) {
        this.length = length;
    }

    public byte getLength() {
        return length;
    }

    public void setValue(RecordValue value) {
        this.value = value;
    }

    public RecordValue getValue() {
        return value;
    }
}