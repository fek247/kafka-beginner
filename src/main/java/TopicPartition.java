import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class TopicPartition extends BaseApi {
    public static int UNKNOWN_TOPIC_OR_PARTITION = 3;

    private byte arrayLength;

    private byte nameLength;
    
    private byte[] name;

    private byte tagBuffer;

    private int partitionLimit;

    private byte cursor;

    public TopicPartition(DataInputStream inputStream, DataOutputStream outputStream)
    {
        this.dataInputStream = inputStream;
        this.dataOutputStream = outputStream;
    }

    private void setArrayLength(byte arrayLength)
    {
        this.arrayLength = arrayLength;
    }

    private void setNameLength(byte nameLength)
    {
        this.nameLength = nameLength;
    }

    private void setName(byte[] name)
    {
        this.name = name;
    }

    private void setTagBuffer(byte tagBuffer)
    {
        this.tagBuffer = tagBuffer;
    }

    private void setPartitionLimit(int partitionLimit)
    {
        this.partitionLimit = partitionLimit;
    }

    private void setCursor(byte cursor)
    {
        this.cursor = cursor;
    }

    @Override
    public void read() {
        if (dataInputStream == null) {
            return;
        }
        try {
            setArrayLength(dataInputStream.readByte());
            setNameLength(dataInputStream.readByte());
            byte[] name = new byte[this.nameLength - 1];
            dataInputStream.read(name);
            setName(name);
            setTagBuffer(dataInputStream.readByte());
            setPartitionLimit(dataInputStream.readInt());
            setCursor(dataInputStream.readByte());
            // Skip last tag buffer, already set above
            dataInputStream.skip(1);
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    @Override
    public void write() {
        if (dataOutputStream == null) {
            return;
        }

        ByteArrayOutputStream byteArrBodyRes = this.responseBody();
        try {
            this.dataOutputStream.writeInt(byteArrBodyRes.size() + 5);
            this.dataOutputStream.writeInt(this.header.getCorrelationId());
            this.dataOutputStream.writeByte(this.header.getTagBuffer());
            this.dataOutputStream.write(byteArrBodyRes.toByteArray());
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    public ByteArrayOutputStream responseBody()
    {
        ByteArrayOutputStream byteArr = new ByteArrayOutputStream();
        DataOutputStream dOut = new DataOutputStream(byteArr);

        try {
            // Throttle Time
            dOut.writeInt(0);
            // Topic Array
                // Array Length
                dOut.writeByte(2);
                // Topic
                    // Error code
                    dOut.writeShort(UNKNOWN_TOPIC_OR_PARTITION);
                    // Topic name
                        // Length
                        dOut.writeByte(this.nameLength);
                        // Contents
                        dOut.write(this.name);
                    // Topic ID
                    byte[] topicId = new byte[16];
                    dOut.write(topicId);
                    // Is Internal
                    dOut.writeByte(0);
                    // Partitions Array
                    dOut.writeByte(1);
                    // Topic Authorized Operations
                    dOut.writeInt(3576);
                    // Tag Buffer
                    dOut.writeByte(this.header.getTagBuffer());
            // Next Cursor
            dOut.writeByte(this.cursor);
            // Tag Buffer
            dOut.writeByte(this.header.getTagBuffer());
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }

        return byteArr;
    }

    public byte getArrayLength()
    {
        return this.arrayLength;
    }

    public byte getTagBuffer()
    {
        return this.tagBuffer;
    }

    public byte[] getName()
    {
        return this.name;
    }

    public byte getNameLength()
    {
        return this.nameLength;
    }

    public int getPartitionLimit()
    {
        return this.partitionLimit;
    }
}