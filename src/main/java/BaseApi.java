

import java.io.DataInputStream;
import java.io.DataOutputStream;

public abstract class BaseApi {
    protected RequestHeader header;

    protected DataInputStream dataInputStream;

    protected DataOutputStream dataOutputStream;

    protected void setDataInputStream(DataInputStream dataInputStream)
    {
        this.dataInputStream = dataInputStream;
    }

    protected void setDataOutputStream(DataOutputStream dataOutputStream)
    {
        this.dataOutputStream = dataOutputStream;
    }

    protected void setHeader(RequestHeader header)
    {
        this.header = header;
    }

    public abstract void read();

    public abstract void write();
}
