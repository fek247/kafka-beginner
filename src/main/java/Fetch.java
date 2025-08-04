
import java.io.DataInputStream;
import java.io.DataOutputStream;

import Fetch.FetchRequest;
import Fetch.FetchResponse;

public class Fetch extends BaseApi {
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

        FetchRequest fetchRequest = new FetchRequest();
        fetchRequest.request(dataInputStream);
    }

    @Override
    public void write() {
        if (dataOutputStream == null) {
            return;
        }

        FetchResponse fetchResponse = new FetchResponse();
        fetchResponse.response(dataOutputStream);
    }
}
