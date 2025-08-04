
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import Fetch.FetchRequest;
import Fetch.FetchResponse;

public class Fetch extends BaseApi {
    private FetchRequest fetchRequest;

    private FetchResponse fetchResponse;

    public Fetch(DataInputStream inputStream, DataOutputStream outputStream)
    {
        this.dataInputStream = inputStream;
        this.dataOutputStream = outputStream;
    }
    @Override
    public void read() {
        if (dataInputStream == null) {
            return;
        }

        this.fetchRequest = new FetchRequest();
        fetchRequest.request(dataInputStream);
    }

    @Override
    public void write() {
        if (dataOutputStream == null) {
            return;
        }

        ByteArrayOutputStream byteArrBodyRes = new ByteArrayOutputStream();
        DataOutputStream dOutBody = new DataOutputStream(byteArrBodyRes);
        this.fetchResponse = new FetchResponse();
        // Should split to function
        this.fetchResponse.setSessionId(this.getFetchRequest().getSessionId());
        this.fetchResponse.setTopicLength(this.fetchRequest.getTopicLength());
        fetchResponse.response(dOutBody);
        try {
            this.dataOutputStream.writeInt(byteArrBodyRes.size() + 5);
            this.dataOutputStream.writeInt(this.header.getCorrelationId());
            this.dataOutputStream.write(byteArrBodyRes.toByteArray());
            this.dataOutputStream.write(byteArrBodyRes.toByteArray());
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    public FetchRequest getFetchRequest() {
        return fetchRequest;
    }

    public void setFetchRequest(FetchRequest fetchRequest) {
        this.fetchRequest = fetchRequest;
    }

    public FetchResponse getFetchResponse() {
        return fetchResponse;
    }

    public void setFetchResponse(FetchResponse fetchResponse) {
        this.fetchResponse = fetchResponse;
    }
}
