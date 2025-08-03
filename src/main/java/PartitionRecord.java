public class PartitionRecord extends Record {
    @Override
    public PartitionRecordValue getValue() {
        return (PartitionRecordValue) value;
    }
}