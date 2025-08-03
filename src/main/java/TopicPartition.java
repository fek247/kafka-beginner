import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TopicPartition extends BaseApi {
    public static int NO_ERROR = 0;

    public static int UNKNOWN_TOPIC_OR_PARTITION = 3;

    private byte arrayLength;

    private byte nameLength;
    
    private byte[] name;

    private byte[] uuid;

    private byte tagBuffer;

    private int partitionLimit;

    private byte cursor;

    private List<Record> records;

    private List<PartitionRecord> partitionRecords;

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

    private void setUUID(byte[] uuid)
    {
        this.uuid = uuid;
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

    public void setRecords(List<Record> records) {
        this.records = records;
    }

    public void setPartitionRecords(List<PartitionRecord> partitionRecords) {
        this.partitionRecords = partitionRecords;
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
            checkValidTopic(Arrays.toString(name));
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
                    if (this.getUUID() == null) {
                        dOut.writeShort(UNKNOWN_TOPIC_OR_PARTITION);
                    } else {
                        dOut.writeShort(NO_ERROR);
                    }
                    // Topic name
                        // Length
                        dOut.writeByte(this.nameLength);
                        // Contents
                        dOut.write(this.name);
                    // Topic ID
                    if (this.getUUID() != null) {
                        dOut.write(this.getUUID());
                    } else {
                        byte[] topicUuid = new byte[16];
                        dOut.write(topicUuid);
                    }
                    // Is Internal
                    dOut.writeByte(0);
                    // Partitions Array
                    dOut.write(this.getPartionRecords().size() + 1);
                    for (PartitionRecord partitionRecord : this.partitionRecords) {
                        PartitionRecordValue value = partitionRecord.getValue();
                        dOut.writeShort(NO_ERROR);
                        dOut.writeInt(value.getPartitionId());
                        dOut.writeInt(value.getLeader());
                        dOut.writeInt(value.getLeaderEpoch());
                        dOut.write(value.getReplicas().length + 1);
                        for (int i = 0; i < value.getReplicas().length; i++) {
                            dOut.writeInt(value.getReplicas()[i]);
                        }
                        dOut.write(value.getInSyncReplicas().length + 1);
                        for (int i = 0; i < value.getInSyncReplicas().length; i++) {
                            dOut.writeInt(value.getInSyncReplicas()[i]);
                        }
                        dOut.write(value.getInSyncReplicas().length);
                        dOut.write(value.getInSyncReplicas().length);
                        dOut.write(value.getOfflineReplicaLength());
                        // Tag buffer
                        dOut.writeByte(this.header.getTagBuffer());
                    }
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

    public void checkValidTopic(String name)
    {
        try {
            String path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
            InputStream inputStream = new FileInputStream(new File(path));
            DataInputStream logDataInputStream = new DataInputStream(inputStream);

            List<Record> records = new ArrayList<>();
            List<PartitionRecord> partitionRecords = new ArrayList<>();
            while ((logDataInputStream.read()) != -1) {
                // Skip next three byte belong to base offset
                byte[] next7Byte = new byte[7]; 
                logDataInputStream.read(next7Byte);

                int batchLength = logDataInputStream.readInt();
                logDataInputStream.skip(45);
                int recordsLength = logDataInputStream.readInt();
                int i = 0;
                while (i < recordsLength) {
                    int recordLength = VarIntReader.readSignedVarInt(logDataInputStream);
                    // Skip Attributes
                    logDataInputStream.skip(1);
                    int timestampDelta = VarIntReader.readSignedVarInt(logDataInputStream);
                    int offsetDelta = VarIntReader.readSignedVarInt(logDataInputStream);
                    int keyLength = VarIntReader.readSignedVarInt(logDataInputStream);
                    if (keyLength == -1) {
                        keyLength = 0;
                    }
                    logDataInputStream.skip(keyLength);
                    int valueLength = VarIntReader.readSignedVarInt(logDataInputStream);
                    // Skip Frame version
                    logDataInputStream.skip(1);
                    byte type = logDataInputStream.readByte();
                    switch (type) {
                        case 2:
                            {
                                // Skip Version
                                logDataInputStream.skip(1);
                                int topicNameLength = VarIntReader.readUnsignedVarInt(logDataInputStream);
                                byte[] topicName = new byte[topicNameLength - 1];
                                logDataInputStream.read(topicName);
                                byte[] topicUUID = new byte[16];
                                logDataInputStream.read(topicUUID);
                                // Skip Tagged Fields Count and Headers Array Count
                                int taggedFieldsCount = VarIntReader.readUnsignedVarInt(logDataInputStream);
                                int headerArrayCount = VarIntReader.readUnsignedVarInt(logDataInputStream);

                                // Set uuid
                                if (Arrays.toString(topicName).equals(name)) {
                                    setUUID(topicUUID);
                                }

                                // Insert to list topic
                                TopicRecordValue topicRecordValue = new TopicRecordValue();
                                topicRecordValue.setNameLength(topicNameLength);
                                topicRecordValue.setName(topicName);
                                topicRecordValue.setUuid(topicUUID);
                                Record topicRecord = new TopicRecord();
                                topicRecord.setValue(topicRecordValue);
                                records.add(topicRecord);
                                break;
                            }
                        case 3:
                            {
                                // Skip Version
                                logDataInputStream.skip(1);
                                int partitionId = logDataInputStream.readInt();
                                byte[] topicUUID = new byte[16];
                                logDataInputStream.read(topicUUID);
                                int replicaLength = VarIntReader.readUnsignedVarInt(logDataInputStream);
                                int[] replicaArr = new int[replicaLength - 1];
                                for (int j = 0; j < replicaArr.length; j++) {
                                    replicaArr[j] = logDataInputStream.readInt();
                                }
                                int inSyncReplicaLength = VarIntReader.readUnsignedVarInt(logDataInputStream);
                                int[] inSyncReplicaArr = new int[inSyncReplicaLength - 1];
                                for (int j = 0; j < inSyncReplicaArr.length; j++) {
                                    inSyncReplicaArr[j] = logDataInputStream.readInt();
                                }
                                int removingReplicaLength = VarIntReader.readUnsignedVarInt(logDataInputStream);
                                int addingReplicaLength = VarIntReader.readUnsignedVarInt(logDataInputStream);
                                int leader = logDataInputStream.readInt();
                                int leaderEpoch = logDataInputStream.readInt();
                                int partitionEpoch = logDataInputStream.readInt();
                                int directoryLength = VarIntReader.readUnsignedVarInt(logDataInputStream);
                                byte[] directoryUUID = new byte[16];
                                logDataInputStream.read(directoryUUID);
                                int taggedFieldsCount = VarIntReader.readUnsignedVarInt(logDataInputStream);
                                int headerArrayCount = VarIntReader.readUnsignedVarInt(logDataInputStream);

                                // Insert to list topic
                                if (Arrays.toString(topicUUID).equals(Arrays.toString(this.getUUID()))) {
                                    PartitionRecordValue partitionRecordValue = new PartitionRecordValue();
                                    partitionRecordValue.setPartitionId(partitionId);
                                    partitionRecordValue.setTopicUUID(topicUUID);
                                    partitionRecordValue.setLeader(leader);
                                    partitionRecordValue.setLeaderEpoch(leaderEpoch);
                                    partitionRecordValue.setReplicas(replicaArr);
                                    partitionRecordValue.setInSyncReplicas(inSyncReplicaArr);
                                    partitionRecordValue.setOfflineReplicaLength(removingReplicaLength);
                                    PartitionRecord partitionRecord = new PartitionRecord();
                                    partitionRecord.setValue(partitionRecordValue);
                                    partitionRecords.add(partitionRecord);
                                }
                                break;
                            }
                        default:
                            {
                                logDataInputStream.skip(valueLength - 2);
                                int headerArrayCount = VarIntReader.readUnsignedVarInt(logDataInputStream);
                                break;
                            }
                    }
                    i++;
                }
            }
            setRecords(records);
            setPartitionRecords(partitionRecords);
            System.out.println("Done read log metadata file");
        } catch (IOException e) {
            System.err.println("Error reading metadata log file: " + e.getMessage());
        }
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

    public byte[] getUUID()
    {
        return this.uuid;
    }

    public byte getNameLength()
    {
        return this.nameLength;
    }

    public int getPartitionLimit()
    {
        return this.partitionLimit;
    }

    public List<PartitionRecord> getPartionRecords()
    {
        return this.partitionRecords;
    }
}