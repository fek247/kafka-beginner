package TopicPartition;

import java.io.DataOutputStream;
import java.io.IOException;

public class PartitionResponse {
    private short errorCode;

    private int partitionId;

    private int leader;

    private int leaderEpoch;

    private int replicasLength;

    private int[] replicas;

    private int inSyncReplicasLength;

    private int[] inSyncReplicas;

    private int eligibleLeaderReplicaLength;

    private int[] eligibleLeaderReplicas;

    private int lastKnowELRLength;

    private int[] lastKnowELR;

    private int offlineReplicaLength;

    private int[] offlineReplicas;

    private byte tagBuffer;

    public void response(DataOutputStream dataOutputStream)
    {
        try {
            dataOutputStream.writeShort(errorCode);
            dataOutputStream.writeInt(partitionId);
            dataOutputStream.writeInt(leader);
            dataOutputStream.writeInt(leaderEpoch);
            // Replica Nodes
            dataOutputStream.write(replicasLength);
            for (int i = 0; i < replicas.length; i++) {
                dataOutputStream.writeInt(replicas[i]);
            }
            // ISR Nodes
            dataOutputStream.write(inSyncReplicasLength);
            for (int i = 0; i < inSyncReplicas.length; i++) {
                dataOutputStream.writeInt(inSyncReplicas[i]);
            }
            //Eligible Leader Replicas
            dataOutputStream.write(eligibleLeaderReplicaLength);
            for (int i = 0; i < eligibleLeaderReplicas.length; i++) {
                dataOutputStream.writeInt(eligibleLeaderReplicas[i]);
            }
            // Last Known ELR
            dataOutputStream.write(lastKnowELRLength);
            for (int i = 0; i < lastKnowELR.length; i++) {
                dataOutputStream.writeInt(lastKnowELR[i]);
            }
            // Offline Replicas
            dataOutputStream.write(offlineReplicaLength);
            for (int i = 0; i < offlineReplicas.length; i++) {
                dataOutputStream.writeInt(offlineReplicas[i]);
            }
            dataOutputStream.writeByte(tagBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public short getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public int getLeader() {
        return leader;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public int getLeaderEpoch() {
        return leaderEpoch;
    }

    public void setLeaderEpoch(int leaderEpoch) {
        this.leaderEpoch = leaderEpoch;
    }

    public int getReplicasLength() {
        return replicasLength;
    }

    public void setReplicasLength(int replicasLength) {
        this.replicasLength = replicasLength;
    }

    public int[] getReplicas() {
        return replicas;
    }

    public void setReplicas(int[] replicas) {
        this.replicas = replicas;
    }

    public int getInSyncReplicasLength() {
        return inSyncReplicasLength;
    }

    public void setInSyncReplicasLength(int inSyncReplicasLength) {
        this.inSyncReplicasLength = inSyncReplicasLength;
    }

    public int[] getInSyncReplicas() {
        return inSyncReplicas;
    }

    public void setInSyncReplicas(int[] inSyncReplicas) {
        this.inSyncReplicas = inSyncReplicas;
    }

    public int getEligibleLeaderReplicaLength() {
        return eligibleLeaderReplicaLength;
    }

    public void setEligibleLeaderReplicaLength(int eligibleLeaderReplicaLength) {
        this.eligibleLeaderReplicaLength = eligibleLeaderReplicaLength;
    }

    public int getLastKnowELRLength() {
        return lastKnowELRLength;
    }

    public void setLastKnowELRLength(int lastKnowELRLength) {
        this.lastKnowELRLength = lastKnowELRLength;
    }

    public int getOfflineReplicaLength() {
        return offlineReplicaLength;
    }

    public void setOfflineReplicaLength(int offlineReplicaLength) {
        this.offlineReplicaLength = offlineReplicaLength;
    }

    public byte getTagBuffer() {
        return tagBuffer;
    }

    public void setTagBuffer(byte tagBuffer) {
        this.tagBuffer = tagBuffer;
    }

    public int[] getEligibleLeaderReplicas() {
        return eligibleLeaderReplicas;
    }

    public void setEligibleLeaderReplicas(int[] eligibleLeaderReplicas) {
        this.eligibleLeaderReplicas = eligibleLeaderReplicas;
    }

    public int[] getLastKnowELR() {
        return lastKnowELR;
    }

    public void setLastKnowELR(int[] lastKnowELR) {
        this.lastKnowELR = lastKnowELR;
    }

    public int[] getOfflineReplicas() {
        return offlineReplicas;
    }

    public void setOfflineReplicas(int[] offlineReplicas) {
        this.offlineReplicas = offlineReplicas;
    }
}
