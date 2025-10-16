package Common;

import java.io.DataOutputStream;
import java.io.IOException;

public class MessageRecordValue extends RecordValue {
    public static byte MESSAGE_TYPE = 0;

    public MessageRecordValue() {
        super(MESSAGE_TYPE);
    }

    private byte[] message;

    public byte[] getMessage() {
        return message;
    }

    public void setMessage(byte[] message) {
        this.message = message;
    }

    @Override
    public void response(DataOutputStream dataOutputStream)
    {
        try {
            dataOutputStream.write(this.message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
