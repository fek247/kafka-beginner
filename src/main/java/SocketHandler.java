import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import Common.ApiKey;
import Common.MetadataLogFile;

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

            while ((dataInputStream.read()) != -1) {
                // Skip next three byte belong to message size
                dataInputStream.skip(3);

                // Read header
                RequestHeader header = new RequestHeader();
                header.setApiKey(dataInputStream.readShort());
                header.setApiVersion(dataInputStream.readShort());
                header.setCorrelationId(dataInputStream.readInt());
                header.setClientLength(dataInputStream.readShort());
                byte[] headerContent = new byte[header.getHeaderLength()];
                dataInputStream.read(headerContent);
                header.setClientContent(headerContent);
                header.setTagBuffer(dataInputStream.readByte());
                if (header.getApiKey() == ApiKey.ApiVersions) {
                    baseBodyApi = new ApiVersion(dataInputStream, dataOutputStream);
                }
                if (header.getApiKey() == ApiKey.DescribeTopicPartitions) {
                    baseBodyApi = new TopicPartition(dataInputStream, dataOutputStream);
                }
                if (header.getApiKey() == ApiKey.Fetch) {
                    baseBodyApi = new Fetch(dataInputStream, dataOutputStream);
                }

                baseBodyApi.setHeader(header);
                baseBodyApi.read();
                baseBodyApi.write();
            }
        } catch (IOException e) {
            System.out.println("Local address: " + socket.getLocalAddress());
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
