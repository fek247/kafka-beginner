package Common;

import java.io.DataOutputStream;

public class RecordValue {
    public final byte type;

    public RecordValue(byte type) {
        this.type = type;
    }

    public byte getType()
    {
        return this.type;
    }

    public void response(DataOutputStream dataOutputStream)
    {
        // Empty
    }
}