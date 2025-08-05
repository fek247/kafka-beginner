package Common;
import Abstract.RecordValue;

public class PartitionRecordValue extends RecordValue {
    public static byte PARTITION_TYPE = 3;

    private int partitionId;

    private byte[] topicUUID;

    private int leader;

    private int leaderEpoch;

    private int[] replicas;

    private int[] inSyncReplicas;

    private int offlineReplicaLength;

    public PartitionRecordValue() 
    {
        super(PARTITION_TYPE);
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public void setTopicUUID(byte[] topicUUID) {
        this.topicUUID = topicUUID;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public void setLeaderEpoch(int leaderEpoch) {
        this.leaderEpoch = leaderEpoch;
    }

    public void setReplicas(int[] replicas) {
        this.replicas = replicas;
    }

    public void setInSyncReplicas(int[] inSyncReplicas) {
        this.inSyncReplicas = inSyncReplicas;
    }

    public void setOfflineReplicaLength(int offlineReplicaLength) {
        this.offlineReplicaLength = offlineReplicaLength;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public byte[] getTopicUUID() {
        return topicUUID;
    }

    public int getLeader() {
        return leader;
    }

    public int getLeaderEpoch() {
        return leaderEpoch;
    }

    public int[] getReplicas() {
        return replicas;
    }

    public int[] getInSyncReplicas() {
        return inSyncReplicas;
    }

    public int getOfflineReplicaLength() {
        return offlineReplicaLength;
    }
}