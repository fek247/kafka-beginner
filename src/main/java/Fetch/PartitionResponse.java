package Fetch;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import Common.RecordBatch;

public class PartitionResponse {
    private int partitionId;

    private short errorCode;

    private long highWatermark;

    private long lastStableOffet;

    private long logStartOffset;

    private int abortTransactionLength;

    private List<AbortTransactionResponse> abortTransactionResponses;

    private int preferredReadReplica;

    private int recordBatchLength;

    // private List<RecordBatchResponse> recordBatchResponses;
    private List<RecordBatch> recordBatchs;

    private byte tagBuffer;
    
    public void response(DataOutputStream dataOutputStream)
    {
        try {
            dataOutputStream.writeInt(partitionId);
            dataOutputStream.writeShort(errorCode);
            dataOutputStream.writeLong(highWatermark);
            dataOutputStream.writeLong(lastStableOffet);
            dataOutputStream.writeLong(logStartOffset);
            dataOutputStream.write(abortTransactionLength);
            for (int i = 0; i < abortTransactionLength; i++) {
                abortTransactionResponses.get(i).response(dataOutputStream);
            }
            dataOutputStream.writeInt(preferredReadReplica);
            dataOutputStream.write(recordBatchLength);
            for (int i = 0; i < recordBatchLength - 1; i++) {
                recordBatchs.get(i).response(dataOutputStream);
            }
            dataOutputStream.writeByte(tagBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
    }

    public void setHighWatermark(long highWatermark) {
        this.highWatermark = highWatermark;
    }

    public void setLastStableOffet(long lastStableOffet) {
        this.lastStableOffet = lastStableOffet;
    }

    public void setLogStartOffset(long logStartOffset) {
        this.logStartOffset = logStartOffset;
    }

    public void setAbortTransactionLength(int abortTransactionLength) {
        this.abortTransactionLength = abortTransactionLength;
    }

    public void setAbortTransactionResponses(List<AbortTransactionResponse> abortTransactionResponses) {
        this.abortTransactionResponses = abortTransactionResponses;
    }

    public void setPreferredReadReplica(int preferredReadReplica) {
        this.preferredReadReplica = preferredReadReplica;
    }

    public void setRecordBatchLength(int recordBatchLength) {
        this.recordBatchLength = recordBatchLength;
    }

    public void setTagBuffer(byte tagBuffer) {
        this.tagBuffer = tagBuffer;
    }

    public void setRecordBatchs(List<RecordBatch> recordBatchs) {
        this.recordBatchs = recordBatchs;
    }
}
