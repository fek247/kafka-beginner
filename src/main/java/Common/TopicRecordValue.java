package Common;

public class TopicRecordValue extends RecordValue {
    public static byte TOPIC_TYPE = 2;

    private byte frameVersion;

    private byte version;

    private int nameLength;

    private byte[] name;

    private byte[] uuid;

    private int taggedFieldsCount;

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

    public byte getFrameVersion() {
        return frameVersion;
    }

    public void setFrameVersion(byte frameVersion) {
        this.frameVersion = frameVersion;
    }

    public byte getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public int getTaggedFieldsCount() {
        return taggedFieldsCount;
    }

    public void setTaggedFieldsCount(int taggedFieldsCount) {
        this.taggedFieldsCount = taggedFieldsCount;
    }
}