package Common;

public class PartitionRecordValue extends RecordValue {
    public static byte PARTITION_TYPE = 3;

    private byte frameVersion;

    private byte version;

    private int partitionId;

    private byte[] topicUUID;

    private int replicaLength;

    private int[] replicas;

    private int inSyncReplicaLength;

    private int[] inSyncReplicas;

    private int removingReplicaLength;

    private int[] removingReplicas;

    private int addingReplicaLength;

    private int[] addingReplicas;

    private int leader;

    private int leaderEpoch;

    private int partitionEpoch;

    private int directoyLength;

    private byte[][] directories;

    private int taggedFieldsCount;

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

    public byte getFrameVersion() {
        return frameVersion;
    }

    public void setFrameVersion(byte frameVersion) {
        this.frameVersion = frameVersion;
    }

    public byte getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public int getReplicaLength() {
        return replicaLength;
    }

    public void setReplicaLength(int replicaLength) {
        this.replicaLength = replicaLength;
    }

    public int getInSyncReplicaLength() {
        return inSyncReplicaLength;
    }

    public void setInSyncReplicaLength(int inSyncReplicaLength) {
        this.inSyncReplicaLength = inSyncReplicaLength;
    }

    public int getRemovingReplicaLength() {
        return removingReplicaLength;
    }

    public void setRemovingReplicaLength(int removingReplicaLength) {
        this.removingReplicaLength = removingReplicaLength;
    }

    public int[] getRemovingReplicas() {
        return removingReplicas;
    }

    public void setRemovingReplicas(int[] removingReplicas) {
        this.removingReplicas = removingReplicas;
    }

    public int getAddingReplicaLength() {
        return addingReplicaLength;
    }

    public void setAddingReplicaLength(int addingReplicaLength) {
        this.addingReplicaLength = addingReplicaLength;
    }

    public int[] getAddingReplicas() {
        return addingReplicas;
    }

    public void setAddingReplicas(int[] addingReplicas) {
        this.addingReplicas = addingReplicas;
    }

    public int getPartitionEpoch() {
        return partitionEpoch;
    }

    public void setPartitionEpoch(int partitionEpoch) {
        this.partitionEpoch = partitionEpoch;
    }

    public int getDirectoyLengh() {
        return directoyLength;
    }

    public void setDirectoyLength(int directoyLength) {
        this.directoyLength = directoyLength;
    }

    public byte[][] getDirectories() {
        return directories;
    }

    public void setDirectories(byte[][] directories) {
        this.directories = directories;
    }

    public int getTaggedFieldsCount() {
        return taggedFieldsCount;
    }

    public void setTaggedFieldsCount(int taggedFieldsCount) {
        this.taggedFieldsCount = taggedFieldsCount;
    }
}