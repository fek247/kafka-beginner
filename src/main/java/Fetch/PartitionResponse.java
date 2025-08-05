package Fetch;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class PartitionResponse {
    private int partitionId;

    private short errorCode;

    private long highWatermark;

    private long lastStableOffet;

    private long logStartOffset;

    private int abortTransactionLength;

    private List<AbortTransactionResponse> abortTransactionResponses;

    private int preferredReadReplica;

    private int recordLength;

    private int[] records;

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
            dataOutputStream.write(recordLength);
            for (int i = 0; i < recordLength; i++) {
                dataOutputStream.writeInt(records[i]);
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

    public void setRecordLength(int recordLength) {
        this.recordLength = recordLength;
    }

    public void setRecords(int[] records) {
        this.records = records;
    }

    public void setTagBuffer(byte tagBuffer) {
        this.tagBuffer = tagBuffer;
    }
}
