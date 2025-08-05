package Fetch;

import java.io.DataOutputStream;
import java.io.IOException;

public class AbortTransactionResponse {
    private long producerId;

    private long firstOffset;

    public void response(DataOutputStream dataOutputStream)
    {
        try {
            dataOutputStream.writeLong(producerId);
            dataOutputStream.writeLong(firstOffset);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long getProducerId() {
        return producerId;
    }

    public void setProducerId(long producerId) {
        this.producerId = producerId;
    }

    public long getFirstOffset() {
        return firstOffset;
    }

    public void setFirstOffset(long firstOffset) {
        this.firstOffset = firstOffset;
    }
}
