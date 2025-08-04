package Fetch;

import java.io.DataOutputStream;

public class FetchResponse {
    private int throttleTimeMs;

    private short errorCode;

    private int sessionId;


    public void response(DataOutputStream dataOutputStream)
    {
        try {
            dataOutputStream.writeInt(throttleTimeMs);
            dataOutputStream.writeShort(errorCode);
            dataOutputStream.writeInt(sessionId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
