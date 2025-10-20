import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import Common.LogFileReader;
import Common.PartitionRecordValue;
import Common.Record;
import Common.TopicRecordValue;
import Constant.ErrorCode;
import Helpers.VarIntReader;
import TopicPartition.PartitionResponse;
import TopicPartition.TopicRequest;
import TopicPartition.TopicResponse;

public class TopicPartition extends BaseApi {
    private int arrayLength;

    private List<TopicRequest> topicRequests;

    private byte nameLength;
    
    private byte[] name;

    private byte[] uuid;

    private byte tagBuffer;

    private int partitionLimit;

    private byte cursor;

    private LogFileReader metadataLogFile;

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

    @Override
    public void read() {
        if (dataInputStream == null) {
            return;
        }
        try {
            // Read metadata log file
            String metadataLogFilePath = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
            LogFileReader metadataLogFile = new LogFileReader();
            metadataLogFile.init(metadataLogFilePath, true);
            setMetadataLogFile(metadataLogFile);

            setArrayLength(VarIntReader.readUnsignedVarInt(dataInputStream));
            List<TopicRequest> topicRequests = new ArrayList<>();
            for (int i = 0; i < this.arrayLength - 1; i++) {
                TopicRequest topicRequest = new TopicRequest();
                topicRequest.request(dataInputStream);
                topicRequests.add(topicRequest);
            }
            topicRequests.sort(Comparator.comparing(TopicRequest::getNameAsString));
            setTopicRequests(topicRequests);
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
                        Record topicRecord = this.metadataLogFile.getTopicInMetadatLog(Arrays.toString(topicRequest.getName()));

                        TopicResponse topicResponse = new TopicResponse();
                        short errorCode = topicRecord == null ? ErrorCode.UNKNOWN_TOPIC_OR_PARTITION : ErrorCode.NO_ERROR;
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
                        if (topicRecord != null) {
                            TopicRecordValue topicRecordValue = (TopicRecordValue) topicRecord.getValue();
                            List<Record> listPartitionRecord = this.metadataLogFile.getListPartitionRecord(topicRecordValue.getUuid());
                            for (Record partitionRecord : listPartitionRecord) {
                                PartitionRecordValue partitionRecordValue = (PartitionRecordValue) partitionRecord.getValue();
                                PartitionResponse partitionResponse = new PartitionResponse();
                                partitionResponse.setErrorCode(ErrorCode.NO_ERROR);
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

    public List<TopicRequest> getTopicRequests() {
        return topicRequests;
    }

    public void setTopicRequests(List<TopicRequest> topicRequests) {
        this.topicRequests = topicRequests;
    }

    public LogFileReader getMetadataLogFile() {
        return metadataLogFile;
    }

    public void setMetadataLogFile(LogFileReader metadataLogFile) {
        this.metadataLogFile = metadataLogFile;
    }
}