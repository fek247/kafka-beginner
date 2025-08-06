package Common;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import Helpers.VarIntReader;
import TopicPartition.TopicRecord;
import TopicPartition.TopicRecordValue;

public class MetadataLogFile {
    private List<TopicRecord> topicRecords;

    private List<PartitionRecord> partitionRecords;

    public void init(String path)
    {
        try {
            InputStream inputStream = new FileInputStream(new File(path));
            DataInputStream logDataInputStream = new DataInputStream(inputStream);

            List<TopicRecord> topicRecords = new ArrayList<>();
            List<PartitionRecord> partitionRecords = new ArrayList<>();

            while ((logDataInputStream.read()) != -1) {
                // Skip next seven byte belong to base offset
                logDataInputStream.skip(7);

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

                                // Insert to list topic
                                TopicRecordValue topicRecordValue = new TopicRecordValue();
                                topicRecordValue.setNameLength(topicNameLength);
                                topicRecordValue.setName(topicName);
                                topicRecordValue.setUuid(topicUUID);
                                TopicRecord topicRecord = new TopicRecord();
                                topicRecord.setValue(topicRecordValue);
                                topicRecords.add(topicRecord);
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
                setTopicRecords(topicRecords);
                setPartitionRecords(partitionRecords);
            }
            System.out.println("Done read log metadata file");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public TopicRecord getTopicInMetadatLog(String name)
    {
        for(TopicRecord topicRecord : this.topicRecords) {
            TopicRecordValue recordValue = (TopicRecordValue) topicRecord.getValue();
            if (Arrays.toString(recordValue.getName()).equals(name)) {
                return topicRecord;
            }
        }

        return null;
    }

    public TopicRecord getTopicInMetadatLog(byte[] topicUUID)
    {
        for(TopicRecord topicRecord : this.topicRecords) {
            TopicRecordValue recordValue = (TopicRecordValue) topicRecord.getValue();
            if (Arrays.toString(recordValue.getUuid()).equals(Arrays.toString(topicUUID))) {
                return topicRecord;
            }
        }

        return null;
    }

    public List<TopicRecord> getTopicRecords() {
        return topicRecords;
    }

    public void setTopicRecords(List<TopicRecord> topicRecords) {
        this.topicRecords = topicRecords;
    }

    public List<PartitionRecord> getPartitionRecords() {
        return partitionRecords;
    }

    public void setPartitionRecords(List<PartitionRecord> partitionRecords) {
        this.partitionRecords = partitionRecords;
    }
}
