

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import Common.MetadataLogFile;
import Common.Record;
import Constant.ErrorCode;
import Fetch.FetchRequest;
import Fetch.FetchResponse;
import Fetch.PartitionRequest;
import Fetch.PartitionResponse;
import Fetch.TopicRequest;
import Fetch.TopicResponse;

public class Fetch extends BaseApi {
    private FetchRequest fetchRequest;

    private FetchResponse fetchResponse;

    private MetadataLogFile metadataLogFile;

    public Fetch(DataInputStream inputStream, DataOutputStream outputStream)
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
        MetadataLogFile metadataLogFile = new MetadataLogFile();
        metadataLogFile.init(metadataLogFilePath);
        setMetadataLogFile(metadataLogFile);
        List<String> filePaths = metadataLogFile.getMessageFileStrings();
        for (String filePath : filePaths) {
            System.out.println(filePath);
        }

        FetchRequest fetchRequest = new FetchRequest();
        fetchRequest.request(dataInputStream);
        setFetchRequest(fetchRequest);
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
        this.fetchResponse = new FetchResponse();
        this.fetchResponse.setThrottleTimeMs(0);
        this.fetchResponse.setErrorCode(ErrorCode.NO_ERROR);
        this.fetchResponse.setSessionId(this.getFetchRequest().getSessionId());
        this.fetchResponse.setTopicLength(this.fetchRequest.getTopicLength());
        System.out.println("Topic length from response: " + this.fetchResponse.getTopicLength());
        List<TopicResponse> topicResponses = new ArrayList<>();
        for (TopicRequest topicRequest : this.fetchRequest.getTopicRequests()) {
            Record topicRecord = this.metadataLogFile.getTopicInMetadatLog(topicRequest.getTopicUUID());
            List<PartitionResponse> partitionResponses = new ArrayList<>();
            short errorCode = topicRecord == null ? ErrorCode.UNKNOWN_TOPIC : ErrorCode.NO_ERROR;

            for (PartitionRequest partitionRequest : topicRequest.getPartitionRequests()) {
                PartitionResponse partitionResponse = new PartitionResponse();
                partitionResponse.setPartitionId(partitionRequest.getPartitionId());
                partitionResponse.setErrorCode(errorCode);
                // TODO
                partitionResponse.setHighWatermark(0);
                partitionResponse.setLastStableOffet(0);
                partitionResponse.setLogStartOffset(0);
                partitionResponse.setAbortTransactionLength(0);
                partitionResponses.add(partitionResponse);
            }

            TopicResponse topicResponse = new TopicResponse();
            topicResponse.setTopicUUID(topicRequest.getTopicUUID());
            topicResponse.setParitionLength(topicRequest.getPartitionLength());
            topicResponse.setPartitionResponses(partitionResponses);
            topicResponses.add(topicResponse);
        }
        System.out.println("throttleTimeMs from request: " + fetchRequest.getMaxWaitMs());
        this.fetchResponse.setTopicResponses(topicResponses);
        this.fetchResponse.setTagBuffer(this.header.getTagBuffer());
        fetchResponse.response(dOutBody);

        return byteArrBodyRes;
    }

    public FetchRequest getFetchRequest() {
        return fetchRequest;
    }

    public void setFetchRequest(FetchRequest fetchRequest) {
        this.fetchRequest = fetchRequest;
    }

    public FetchResponse getFetchResponse() {
        return fetchResponse;
    }

    public void setFetchResponse(FetchResponse fetchResponse) {
        this.fetchResponse = fetchResponse;
    }
    public MetadataLogFile getMetadataLogFile() {
        return metadataLogFile;
    }
    public void setMetadataLogFile(MetadataLogFile metadataLogFile) {
        this.metadataLogFile = metadataLogFile;
    }
}
