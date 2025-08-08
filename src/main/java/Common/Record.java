package Common;

import java.io.DataOutputStream;
import java.io.IOException;

import Helpers.VarIntReader;

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
            dataOutputStream.write(VarIntReader.encodeSignedVarInt(length));
            dataOutputStream.writeByte(attributes);
            dataOutputStream.write(timestampDelta);
            dataOutputStream.write(offsetDelta);
            dataOutputStream.write(keyLength);
            dataOutputStream.write(key);
            dataOutputStream.write(VarIntReader.encodeSignedVarInt(valueLength));
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