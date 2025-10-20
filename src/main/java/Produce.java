

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import Common.LogFileReader;
import Common.Record;
import Common.RecordBatch;
import Common.TopicRecordValue;
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
            Record topicRecord = this.metadataLogFile.getTopicInMetadatLog(Arrays.toString(topicRequest.getName()));
            List<PartitionResponse> partitionResponses = new ArrayList<>();
            short errorCode = ErrorCode.UNKNOWN_TOPIC_OR_PARTITION;
            long baseOffset = -1, logAppendTime = -1, logStartOffset = -1;

            for (PartitionRequest partitionRequest : topicRequest.getPartitionRequests()) {
                int partitionId = partitionRequest.getPartitionIndex();
                if (topicRecord != null) {
                    TopicRecordValue topicRecordValue = (TopicRecordValue) topicRecord.getValue();
                    if (this.metadataLogFile.checkPartitionRecordExists(topicRecordValue.getUuid(), partitionId)) {
                        errorCode = ErrorCode.NO_ERROR;
                        baseOffset = 0;
                        logStartOffset = 0;
                    }
                }

                // Store record to log file <log-dir>/<topic-name>-<partition-index>/00000000000000000000.log
                storeRecord(partitionRequest.getRecordBatch(), new String(topicRequest.getName(), StandardCharsets.UTF_8), partitionId);

                PartitionResponse partitionResponse = new PartitionResponse();
                partitionResponse.setPartitionId(partitionRequest.getPartitionIndex());
                partitionResponse.setErrorCode(errorCode);
                partitionResponse.setBaseOffset(baseOffset);
                partitionResponse.setLogAppendTime(logAppendTime);
                partitionResponse.setLogStartOffset(logStartOffset);
                partitionResponse.setRecordErrorLength((byte) 0);
                partitionResponse.setErrorMessage((byte) 0);
                partitionResponse.setTagBuffer(this.header.getTagBuffer());
                partitionResponses.add(partitionResponse);
            }

            TopicResponse topicResponse = new TopicResponse();
            topicResponse.setNameLength(topicRequest.getNameLength());
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

    private void storeRecord(RecordBatch recordBatch, String topicName, int partitionId)
    {
        try {
            String logFilePath = "/tmp/kraft-combined-logs/" + topicName + "-" + partitionId + "/00000000000000000000.log";
            OutputStream outputStream = new FileOutputStream(new File(logFilePath));
            DataOutputStream dataOutputStream = new DataOutputStream(outputStream); 
            recordBatch.response(dataOutputStream);
        } catch(IOException e) {
            e.printStackTrace();
        }
        
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
