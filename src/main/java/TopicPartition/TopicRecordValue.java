package TopicPartition;

import Abstract.RecordValue;

public class TopicRecordValue extends RecordValue {
    public static byte TOPIC_TYPE = 2;

    private int nameLength;

    private byte[] name;

    private byte[] uuid;

    public TopicRecordValue()
    {
        super(TOPIC_TYPE);
    }

    public void setNameLength(int nameLength) {
        this.nameLength = nameLength;
    }

    public void setName(byte[] name) {
        this.name = name;
    }

    public void setUuid(byte[] uuid) {
        this.uuid = uuid;
    }

    public int getNameLength() {
        return nameLength;
    }

    public byte[] getName() {
        return name;
    }

    public byte[] getUuid() {
        return uuid;
    }
}