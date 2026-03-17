import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import Constant.ApiKey;

public class SocketHandler extends Thread {
    private Socket socket;

    public SocketHandler(Socket socket)
    {
        this.socket = socket;
    }

    @Override
    public void run()
    {
        try {
            InputStream inputStream = socket.getInputStream();
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            OutputStream outputStream = socket.getOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
            BaseApi baseBodyApi = null;

            while (true) {
                int messageSize;
                try {
                    messageSize = dataInputStream.readInt();
                } catch (EOFException e) {
                    break;
                }

                // Read header
                RequestHeader header = new RequestHeader();
                header.setApiKey(dataInputStream.readShort());
                header.setApiVersion(dataInputStream.readShort());
                header.setCorrelationId(dataInputStream.readInt());
                header.setClientLength(dataInputStream.readShort());
                byte[] headerContent = new byte[header.getClientLength()];
                dataInputStream.read(headerContent);
                header.setClientContent(headerContent);
                header.setTagBuffer(dataInputStream.readByte());

                if (header.getApiKey() == ApiKey.Produce) {
                    baseBodyApi = new Produce(dataInputStream, dataOutputStream);
                }
                if (header.getApiKey() == ApiKey.ApiVersions) {
                    baseBodyApi = new ApiVersion(dataInputStream, dataOutputStream);
                }
                if (header.getApiKey() == ApiKey.DescribeTopicPartitions) {
                    baseBodyApi = new TopicPartition(dataInputStream, dataOutputStream);
                }
                if (header.getApiKey() == ApiKey.Fetch) {
                    baseBodyApi = new Fetch(dataInputStream, dataOutputStream);
                }

                if (baseBodyApi == null) {
                    System.err.println("Unsupported API Key: " + header.getApiKey());
                }

                baseBodyApi.setHeader(header);
                baseBodyApi.read();
                baseBodyApi.write();
                dataOutputStream.flush();
            }
        } catch (IOException e) {
            System.out.println("Local address: " + socket.getLocalAddress());
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
