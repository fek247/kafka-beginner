package Fetch;

public class PartitionRequest {
    private int partitionId;

    private int leader;

    private long fetchOffset;

    private int lastFetchedOffset;

    private long logStartOffset;

    private int partitionMaxBytes;

    private byte tagBuffer;
}
