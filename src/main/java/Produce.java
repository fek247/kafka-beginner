

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import Common.LogFileReader;
import Common.Record;
import Constant.ErrorCode;
import Produce.PartitionRequest;
import Produce.PartitionResponse;
import Produce.ProduceRequest;
import Produce.ProduceResponse;
import Produce.TopicRequest;
import Produce.TopicResponse;

public class Produce extends BaseApi {
    private ProduceRequest produceRequest;

    private ProduceResponse produceResponse;

    private LogFileReader metadataLogFile;

    public Produce(DataInputStream inputStream, DataOutputStream outputStream)
    {
        this.dataInputStream = inputStream;
        this.dataOutputStream = outputStream;
    }

    @Override
    public void read() {
        if (dataInputStream == null) {
            return;
        }

        // Read metadata
        String metadataLogFilePath = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
        LogFileReader metadataLogFile = new LogFileReader();
        metadataLogFile.init(metadataLogFilePath, true);
        setMetadataLogFile(metadataLogFile);

        ProduceRequest produceRequest = new ProduceRequest();
        produceRequest.request(dataInputStream);
        setProduceRequest(produceRequest);
    }

    @Override
    public void write() {
        if (dataOutputStream == null) {
            return;
        }

        try {
            ByteArrayOutputStream bodyResponse = this.initBodyResponseData();
            this.dataOutputStream.writeInt(bodyResponse.size() + 5);
            this.dataOutputStream.writeInt(this.header.getCorrelationId());
            this.dataOutputStream.write(this.header.getTagBuffer());
            this.dataOutputStream.write(bodyResponse.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private ByteArrayOutputStream initBodyResponseData()
    {
        ByteArrayOutputStream byteArrBodyRes = new ByteArrayOutputStream();
        DataOutputStream dOutBody = new DataOutputStream(byteArrBodyRes);
        this.produceResponse = new ProduceResponse();
        this.produceResponse.setTopicLength(this.produceRequest.getTopicLength());
        List<TopicResponse> topicResponses = new ArrayList<>();
        for (TopicRequest topicRequest : this.produceRequest.getTopicRequests()) {
            Record topicRecord = this.metadataLogFile.getTopicInMetadatLog(topicRequest.getName());
            List<PartitionResponse> partitionResponses = new ArrayList<>();
            short errorCode = topicRecord == null ? ErrorCode.UNKNOWN_TOPIC : ErrorCode.NO_ERROR;

            for (PartitionRequest partitionRequest : topicRequest.getPartitionRequests()) {
                PartitionResponse partitionResponse = new PartitionResponse();
                partitionResponse.setPartitionId(partitionRequest.getPartitionIndex());
                partitionResponse.setErrorCode(errorCode);
                partitionResponse.setBaseOffset(0);
                partitionResponse.setLogAppendTime(0);
                partitionResponse.setLogStartOffset(0);
                partitionResponse.setRecordErrorLength((byte) 0);
                partitionResponse.setErrorMessage((byte) 0);
                partitionResponse.setTagBuffer(this.header.getTagBuffer());
                // String topicName = this.metadataLogFile.getTopicName(topicRequest.getTopicUUID());
                // if (topicName != null) {
                //     String topicFileLog = "/tmp/kraft-combined-logs/" + topicName + "-" + partitionRequest.getPartitionIndex() + "/00000000000000000000.log";
                //     LogFileReader topicFileReader = new LogFileReader();
                //     topicFileReader.init(topicFileLog, false);
                //     int totalRecords = 0;
                //     for (RecordBatch recordBatch : topicFileReader.getRecordBatchs()) {
                //         totalRecords += 12 + recordBatch.getBatchLength();
                //     }
                //     partitionResponse.setRecordCompactLength(VarIntReader.encodeUnsignedVarInt(totalRecords + 1));
                //     partitionResponse.setRecordBatchs(topicFileReader.getRecordBatchs());
                // } else {
                //     partitionResponse.setRecordCompactLength(new byte[]{ 0x00 });
                //     partitionResponse.setRecordBatchs(new ArrayList<>());
                // }
                partitionResponses.add(partitionResponse);
            }

            TopicResponse topicResponse = new TopicResponse();
            topicResponse.setName(topicRequest.getName());
            topicResponse.setPartitionLength(topicRequest.getPartitionLength());
            topicResponse.setPartitionResponses(partitionResponses);
            topicResponses.add(topicResponse);
        }
        this.produceResponse.setTopicResponses(topicResponses);
        this.produceResponse.setThrottleTime(0);
        this.produceResponse.setTagBuffer(this.header.getTagBuffer());
        produceResponse.response(dOutBody);

        return byteArrBodyRes;
    }

    public ProduceRequest getProduceRequest() {
        return produceRequest;
    }

    public void setProduceRequest(ProduceRequest produceRequest) {
        this.produceRequest = produceRequest;
    }

    public ProduceResponse getProduceResponse() {
        return produceResponse;
    }

    public void setProduceResponse(ProduceResponse produceResponse) {
        this.produceResponse = produceResponse;
    }

    public LogFileReader getMetadataLogFile() {
        return metadataLogFile;
    }
    public void setMetadataLogFile(LogFileReader metadataLogFile) {
        this.metadataLogFile = metadataLogFile;
    }
}
