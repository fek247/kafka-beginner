package TopicPartition;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import Helpers.VarIntReader;

public class TopicRequest {
    private int nameLength;

    private byte[] name;

    private byte tagBuffer;

    public void request(DataInputStream dataInputStream)
    {
        try {
            setNameLength(VarIntReader.readUnsignedVarInt(dataInputStream));
            byte[] name = new byte[nameLength - 1];
            dataInputStream.read(name);
            setName(name);
            byte tagBuffer = dataInputStream.readByte();
            setTagBuffer(tagBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getNameLength() {
        return nameLength;
    }

    public void setNameLength(int nameLength) {
        this.nameLength = nameLength;
    }

    public byte[] getName() {
        return name;
    }

    public void setName(byte[] name) {
        this.name = name;
    }

    public byte getTagBuffer() {
        return tagBuffer;
    }

    public void setTagBuffer(byte tagBuffer) {
        this.tagBuffer = tagBuffer;
    }

    public String getNameAsString() {
        return new String(name, StandardCharsets.UTF_8);
    }
}
