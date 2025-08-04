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

import Helpers.VarIntReader;

public class TopicPartition extends BaseApi {
    public static short NO_ERROR = 0;

    public static short UNKNOWN_TOPIC_OR_PARTITION = 3;

    private int arrayLength;

    private List<TopicRequest> topicRequests;

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

    private void setArrayLength(int arrayLength)
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
            setArrayLength(VarIntReader.readUnsignedVarInt(dataInputStream));
            List<TopicRequest> topicRequests = new ArrayList<>();
            for (int i = 0; i < this.arrayLength - 1; i++) {
                TopicRequest topicRequest = new TopicRequest();
                topicRequest.request(dataInputStream);
                topicRequests.add(topicRequest);
            }
            setTopicRequests(topicRequests);
            checkValidTopic(Arrays.toString(name));
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
                dOut.write(this.arrayLength);
                // Topic
                    for (TopicRequest topicRequest : this.topicRequests) {
                        TopicRecord topicRecord = (TopicRecord) this.getTopicInMetadatLog(Arrays.toString(topicRequest.getName()));

                        TopicResponse topicResponse = new TopicResponse();
                        short errorCode = topicRecord == null ? UNKNOWN_TOPIC_OR_PARTITION : NO_ERROR;
                        topicResponse.setErrorCode(errorCode);
                        topicResponse.setTopicNameLength(topicRequest.getNameLength());
                        topicResponse.setTopicName(topicRequest.getName());
                        if (topicRecord != null) {
                            TopicRecordValue topicRecordValue = (TopicRecordValue) topicRecord.getValue();
                            topicResponse.setTopicUUID(topicRecordValue.getUuid());
                        } else {
                            byte[] unknowTopicUUID = new byte[16];
                            topicResponse.setTopicUUID(unknowTopicUUID);
                        }
                        topicResponse.setInternal(false);
                        // Partition Array
                        List<PartitionResponse> listPartitionResponses = new ArrayList<>();
                        for (PartitionRecord partitionRecord : this.partitionRecords) {
                            PartitionRecordValue partitionRecordValue = partitionRecord.getValue();
                            if (Arrays.toString(partitionRecordValue.getTopicUUID()).equals(Arrays.toString(topicResponse.getTopicUUID()))) {
                                PartitionResponse partitionResponse = new PartitionResponse();
                                partitionResponse.setErrorCode(NO_ERROR);
                                partitionResponse.setPartitionId(partitionRecordValue.getPartitionId());
                                partitionResponse.setLeader(partitionRecordValue.getLeader());
                                partitionResponse.setLeaderEpoch(partitionRecordValue.getLeaderEpoch());
                                partitionResponse.setReplicasLength(partitionRecordValue.getReplicas().length + 1);
                                partitionResponse.setReplicas(partitionRecordValue.getReplicas());
                                partitionResponse.setInSyncReplicasLength(partitionRecordValue.getInSyncReplicas().length + 1);
                                partitionResponse.setInSyncReplicas(partitionRecordValue.getInSyncReplicas());
                                partitionResponse.setEligibleLeaderReplicaLength(partitionRecordValue.getInSyncReplicas().length);
                                int[] emptyIntegerArr = new int[0];
                                partitionResponse.setEligibleLeaderReplicas(emptyIntegerArr);
                                partitionResponse.setLastKnowELRLength(partitionRecordValue.getInSyncReplicas().length);
                                partitionResponse.setLastKnowELR(emptyIntegerArr);
                                partitionResponse.setOfflineReplicaLength(partitionRecordValue.getInSyncReplicas().length);
                                partitionResponse.setOfflineReplicas(emptyIntegerArr);
                                partitionResponse.setTagBuffer(this.tagBuffer);
                                listPartitionResponses.add(partitionResponse);
                            }
                        }
                        topicResponse.setPartitionsLength(listPartitionResponses.size() + 1);
                        topicResponse.setPartitionsResponse(listPartitionResponses);
                        topicResponse.setTopicAuthorizedOperations(3567);
                        topicResponse.setTagBuffer(this.tagBuffer);
                        // Write to data stream
                        topicResponse.response(dOut);
                    }
            // Next cursor
            dOut.writeByte(this.cursor);
            // Tag buffer
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
            }
            setRecords(records);
            setPartitionRecords(partitionRecords);
            System.out.println("Done read log metadata file");
        } catch (IOException e) {
            System.err.println("Error reading metadata log file: " + e.getMessage());
        }
    }

    public int getArrayLength()
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

    public List<TopicRequest> getTopicRequests() {
        return topicRequests;
    }

    public void setTopicRequests(List<TopicRequest> topicRequests) {
        this.topicRequests = topicRequests;
    }

    public Record getTopicInMetadatLog(String name)
    {
        for(Record record : this.records) {
            TopicRecordValue recordValue = (TopicRecordValue) record.getValue();
            if (Arrays.toString(recordValue.getName()).equals(name)) {
                return record;
            }
        }

        return null;
    }
}