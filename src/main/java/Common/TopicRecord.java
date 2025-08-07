package Common;

public class TopicRecord extends Record {
    @Override
    public TopicRecordValue getValue() {
        return (TopicRecordValue) value;
    }
}