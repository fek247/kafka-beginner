package Common;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import Helpers.VarIntReader;

public class MetadataLogFile {
    private List<RecordBatch> recordBatchs;

    public void init(String path)
    {
        try {
            InputStream inputStream = new FileInputStream(new File(path));
            DataInputStream logDataInputStream = new DataInputStream(inputStream);

            List<RecordBatch> recordBatchs = new ArrayList<>();
            byte firstByte;

            while ((firstByte = (byte) logDataInputStream.read()) != -1) {
                RecordBatch recordBatch = new RecordBatch();

                byte[] next7Byte = new byte[7];
                logDataInputStream.read(next7Byte);
                ByteBuffer buffer = ByteBuffer.allocate(8);
                buffer.order(ByteOrder.BIG_ENDIAN);
                buffer.put(firstByte);
                buffer.put(next7Byte);
                buffer.flip();
                recordBatch.setBaseOffset(buffer.getLong());

                recordBatch.setBatchLength(logDataInputStream.readInt());
                recordBatch.setPartitionLeaderEpoch(logDataInputStream.readInt());
                recordBatch.setMagicByte(logDataInputStream.readByte());
                recordBatch.setCrc(logDataInputStream.readInt());
                recordBatch.setAttributes(logDataInputStream.readShort());
                recordBatch.setLastOffsetDelta(logDataInputStream.readInt());
                recordBatch.setBaseTimestamp(logDataInputStream.readLong());
                recordBatch.setMaxTimestamp(logDataInputStream.readLong());
                recordBatch.setProducerId(logDataInputStream.readLong());
                recordBatch.setProducerEpoch(logDataInputStream.readShort());
                recordBatch.setBaseSequence(logDataInputStream.readInt());
                recordBatch.setRecordLength(logDataInputStream.readInt());

                List<Record> records = new ArrayList<>();
                int i = 0;
                while (i < recordBatch.getRecordLength()) {
                    Record record = new Record();
                    record.setLength(VarIntReader.readSignedVarInt(logDataInputStream));
                    record.setAttributes(logDataInputStream.readByte());
                    record.setTimestampDelta(VarIntReader.readSignedVarInt(logDataInputStream));
                    record.setOffsetDelta(VarIntReader.readSignedVarInt(logDataInputStream));
                    int keyLength = VarIntReader.readSignedVarInt(logDataInputStream);
                    record.setKeyLength(keyLength);
                    if (keyLength == -1) {
                        keyLength = 0;
                    }
                    byte[] key = new byte[keyLength];
                    logDataInputStream.read(key);
                    record.setKey(key);
                    record.setValueLength(VarIntReader.readSignedVarInt(logDataInputStream));
                    byte frameVersion = logDataInputStream.readByte();
                    byte type = logDataInputStream.readByte();
                    switch (type) {
                        case 2:
                            {
                                TopicRecordValue topicRecordValue = new TopicRecordValue();
                                topicRecordValue.setFrameVersion(frameVersion);
                                topicRecordValue.setVersion(logDataInputStream.readByte());
                                int topicNameLength = VarIntReader.readUnsignedVarInt(logDataInputStream);
                                topicRecordValue.setNameLength(topicNameLength);
                                byte[] topicName = new byte[topicNameLength - 1];
                                logDataInputStream.read(topicName);
                                topicRecordValue.setName(topicName);
                                byte[] topicUUID = new byte[16];
                                logDataInputStream.read(topicUUID);
                                topicRecordValue.setUuid(topicUUID);
                                topicRecordValue.setTaggedFieldsCount(VarIntReader.readUnsignedVarInt(logDataInputStream));

                                record.setValue(topicRecordValue);
                                break;
                            }
                        case 3:
                            {
                                PartitionRecordValue partitionRecordValue = new PartitionRecordValue();
                                partitionRecordValue.setFrameVersion(frameVersion);
                                partitionRecordValue.setVersion(logDataInputStream.readByte());
                                partitionRecordValue.setPartitionId(logDataInputStream.readInt());
                                byte[] topicUUID = new byte[16];
                                logDataInputStream.read(topicUUID);
                                partitionRecordValue.setTopicUUID(topicUUID);

                                // Set replicas
                                partitionRecordValue.setReplicaLength(VarIntReader.readUnsignedVarInt(logDataInputStream));
                                int[] replicaArr = new int[partitionRecordValue.getReplicaLength() - 1];
                                for (int j = 0; j < replicaArr.length; j++) {
                                    replicaArr[j] = logDataInputStream.readInt();
                                }
                                partitionRecordValue.setReplicas(replicaArr);

                                // Set in-sync replicas
                                partitionRecordValue.setInSyncReplicaLength(VarIntReader.readUnsignedVarInt(logDataInputStream));
                                int[] inSyncReplicaArr = new int[partitionRecordValue.getInSyncReplicaLength() - 1];
                                for (int j = 0; j < inSyncReplicaArr.length; j++) {
                                    inSyncReplicaArr[j] = logDataInputStream.readInt();
                                }
                                partitionRecordValue.setInSyncReplicas(inSyncReplicaArr);

                                // Set reming replicas
                                partitionRecordValue.setRemovingReplicaLength(VarIntReader.readUnsignedVarInt(logDataInputStream));
                                int[] removingReplicaArr = new int[partitionRecordValue.getRemovingReplicaLength() - 1];
                                for (int j = 0; j < removingReplicaArr.length; j++) {
                                    removingReplicaArr[j] = logDataInputStream.readInt();
                                }
                                partitionRecordValue.setRemovingReplicas(removingReplicaArr);

                                // Set adding replicas
                                partitionRecordValue.setAddingReplicaLength(VarIntReader.readUnsignedVarInt(logDataInputStream));
                                int[] addingReplicas = new int[partitionRecordValue.getAddingReplicaLength() - 1];
                                for (int j = 0; j < addingReplicas.length; j++) {
                                    addingReplicas[j] = logDataInputStream.readInt();
                                }
                                partitionRecordValue.setAddingReplicas(addingReplicas);

                                partitionRecordValue.setLeader(logDataInputStream.readInt());
                                partitionRecordValue.setLeaderEpoch(logDataInputStream.readInt());
                                partitionRecordValue.setPartitionEpoch(logDataInputStream.readInt());

                                // Set directory
                                partitionRecordValue.setDirectoyLength(VarIntReader.readUnsignedVarInt(logDataInputStream));
                                byte[][] directories = new byte[partitionRecordValue.getDirectoyLengh() - 1][];
                                for (int j = 0; j < directories.length; j++) {
                                    byte[] uuid = new byte[16];
                                    logDataInputStream.read(uuid);
                                    directories[j] = uuid;
                                }
                                partitionRecordValue.setDirectories(directories);
                                partitionRecordValue.setTaggedFieldsCount(VarIntReader.readUnsignedVarInt(logDataInputStream));

                                record.setValue(partitionRecordValue);
                                break;
                            }
                        case 12:
                            {
                                FeatureLevelRecordValue featureLevelRecordValue = new FeatureLevelRecordValue();
                                featureLevelRecordValue.setFrameVersion(frameVersion);
                                featureLevelRecordValue.setVersion(logDataInputStream.readByte());
                                int nameLength = VarIntReader.readUnsignedVarInt(logDataInputStream);
                                featureLevelRecordValue.setNameLength(nameLength);
                                byte[] name = new byte[nameLength - 1];
                                logDataInputStream.read(name);
                                featureLevelRecordValue.setName(name);
                                featureLevelRecordValue.setFeatureLevel(logDataInputStream.readShort());
                                featureLevelRecordValue.setTaggedFieldsCount(VarIntReader.readUnsignedVarInt(logDataInputStream));

                                record.setValue(featureLevelRecordValue);
                                break;
                            }
                        default:
                            System.out.println("Don't support record type: " + type);
                            break;
                    }
                    record.setHeaderArrayCount(VarIntReader.readUnsignedVarInt(logDataInputStream));
                    records.add(record);
                    i++;
                }
                recordBatch.setRecords(records);
                recordBatchs.add(recordBatch);
            }
            this.setRecordBatchs(recordBatchs);
            System.out.println("Done read log metadata file");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Record getTopicInMetadatLog(String name)
    {
        for (RecordBatch recordBatch : this.recordBatchs) {
            for (Record record : recordBatch.getRecords()) {
                if (record.getValue().getType() == 2) {
                    TopicRecordValue recordValue = (TopicRecordValue) record.getValue();
                    if (Arrays.toString(recordValue.getName()).equals(name)) {
                        return record;
                    }
                }
            }
        }

        return null;
    }

    public Record getTopicInMetadatLog(byte[] topicUUID)
    {
        for (RecordBatch recordBatch : this.recordBatchs) {
            for (Record record : recordBatch.getRecords()) {
                if (record.getValue().getType() == 2) {
                    TopicRecordValue recordValue = (TopicRecordValue) record.getValue();
                    if (Arrays.toString(recordValue.getUuid()).equals(Arrays.toString(topicUUID))) {
                        return record;
                    }
                }
            }
        }

        return null;
    }

    public Record getPartitionRecordByUUID(byte[] topicUUID)
    {
        for (RecordBatch recordBatch : this.recordBatchs) {
            for (Record record : recordBatch.getRecords()) {
                if (record.getValue().getType() == 3) {
                    PartitionRecordValue recordValue = (PartitionRecordValue) record.getValue();
                    if (Arrays.toString(recordValue.getTopicUUID()).equals(Arrays.toString(topicUUID))) {
                        return record;
                    }
                }
            }
        }

        return null;
    }

    public List<RecordBatch> getRecordBatchs() {
        return recordBatchs;
    }

    public void setRecordBatchs(List<RecordBatch> recordBatchs) {
        this.recordBatchs = recordBatchs;
    }
}
