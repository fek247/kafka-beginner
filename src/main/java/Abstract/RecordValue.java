package Abstract;

public abstract class RecordValue {
    public final byte type;

    public RecordValue(byte type) {
        this.type = type;
    }

    public byte getType()
    {
        return this.type;
    }
}