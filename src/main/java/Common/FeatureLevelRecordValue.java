package Common;

public class FeatureLevelRecordValue extends RecordValue {
    public static byte FEATURE_LEVEL_TYPE = 12;

    private byte frameVersion;

    private byte version;

    private int nameLength;

    private byte[] name;

    private short featureLevel;

    private int taggedFieldsCount;

    public FeatureLevelRecordValue()
    {
        super(FEATURE_LEVEL_TYPE);
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

    public short getFeatureLevel() {
        return featureLevel;
    }

    public void setFeatureLevel(short featureLevel) {
        this.featureLevel = featureLevel;
    }

    public int getTaggedFieldsCount() {
        return taggedFieldsCount;
    }

    public void setTaggedFieldsCount(int taggedFieldsCount) {
        this.taggedFieldsCount = taggedFieldsCount;
    }
}
