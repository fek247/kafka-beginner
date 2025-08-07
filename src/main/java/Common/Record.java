package Common;

import java.io.DataOutputStream;
import java.io.IOException;

public class Record {
    protected int length;

    private byte attributes;

    // Var signed int
    private int timestampDelta;

    // Var signed int
    private int offsetDelta;

    // Var signed int
    private int keyLength;

    private byte[] key;

    // Var signed int
    private int valueLength;

    protected RecordValue value;

    // Var unsigned int
    private int headerArrayCount;

    public void response(DataOutputStream dataOutputStream)
    {
        try {
            dataOutputStream.write(length);
            System.out.println("Length: " + length);
            dataOutputStream.writeByte(attributes);
            System.out.println("attributes: " + attributes);
            dataOutputStream.write(timestampDelta);
            System.out.println("timestampDelta: " + timestampDelta);
            dataOutputStream.write(offsetDelta);
            System.out.println("offsetDelta: " + offsetDelta);
            dataOutputStream.write(keyLength);
            System.out.println("keyLength: " + keyLength);
            dataOutputStream.write(key);
            System.out.println("Key length" + key.length);
            dataOutputStream.write(valueLength);
            System.out.println("valueLength: " + valueLength);
            value.response(dataOutputStream);
            dataOutputStream.write(headerArrayCount);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getLength() {
        return length;
    }

    public void setValue(RecordValue value) {
        this.value = value;
    }

    public RecordValue getValue() {
        return value;
    }

    public byte getAttributes() {
        return attributes;
    }

    public void setAttributes(byte attributes) {
        this.attributes = attributes;
    }

    public int getTimestampDelta() {
        return timestampDelta;
    }

    public void setTimestampDelta(int timestampDelta) {
        this.timestampDelta = timestampDelta;
    }

    public int getOffsetDelta() {
        return offsetDelta;
    }

    public void setOffsetDelta(int offsetDelta) {
        this.offsetDelta = offsetDelta;
    }

    public int getKeyLength() {
        return keyLength;
    }

    public void setKeyLength(int keyLength) {
        this.keyLength = keyLength;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public int getValueLength() {
        return valueLength;
    }

    public void setValueLength(int valueLength) {
        this.valueLength = valueLength;
    }

    public int getHeaderArrayCount() {
        return headerArrayCount;
    }

    public void setHeaderArrayCount(int headerArrayCount) {
        this.headerArrayCount = headerArrayCount;
    }
}