import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.err.println("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 9092;
    try {
		serverSocket = new ServerSocket(port);
		// Since the tester restarts your program quite often, setting SO_REUSEADDR
		// ensures that we don't run into 'Address already in use' errors
		serverSocket.setReuseAddress(true);
		// Wait for connection from client.
      	clientSocket = serverSocket.accept();

        // Read
        InputStream inputStream = clientSocket.getInputStream();
        DataInputStream dIn = new DataInputStream(inputStream);

        int messageSize = dIn.readInt();
        System.out.println("Message size: " + messageSize);

        short requestApiKey = dIn.readShort();
        short requestApiVersion = dIn.readShort();
        System.out.println("request_api_key: " + requestApiKey);
        System.out.println("request_api_version: " +requestApiVersion);

        int correlationID = dIn.readInt();
        System.out.println("correlationID: " + correlationID);

        short clientHeaderLength = dIn.readShort();
        System.out.println("clientHeaderLength: " + clientHeaderLength);

        byte[] clientHeaderContent = new byte[9];
        dIn.read(clientHeaderContent);
        System.out.println("clientHeaderContent: " + Arrays.toString(clientHeaderContent));

        byte tagBuffer = dIn.readByte();
        System.out.println("tagBuffer: " + tagBuffer);

        // skip clientId on Request body, read above
        byte clientBodyLength = dIn.readByte();
        System.out.println("clientBodyLength: " + clientBodyLength);

        byte[] clientBodyContent = new byte[9];
        dIn.read(clientBodyContent);
        System.out.println("clientBodyContent: " + Arrays.toString(clientBodyContent));

        byte softwareVersionLength = dIn.readByte();
        System.out.println("softwareVersionLength: " + softwareVersionLength);

        byte[] softwareVersionContent = new byte[3];
        dIn.read(softwareVersionContent);
        System.out.println("softwareVersionContent: " + Arrays.toString(softwareVersionContent));

        // skip tagBuffer on Request body, read above
        dIn.skip(1);

        // Write
        ByteArrayOutputStream byteArrBodyRes = new ByteArrayOutputStream();
        DataOutputStream dOut = new DataOutputStream(byteArrBodyRes);

        // Response body
        int code = (requestApiVersion < 0 || requestApiVersion > 4) ? 35 : 0;
        // Error code
        dOut.writeShort(code);

        // API Version Array
            // Array Length
            dOut.writeByte(softwareVersionLength + 1);

            // API Version #1
                // API Key
                dOut.writeShort(1);
                // Min Supported API Version
                dOut.writeShort(0);
                // Max Supported API Version
                dOut.writeShort(17);
                // Tag Buffer
                dOut.writeByte(tagBuffer);

            // API Version #2
                // API Key
                dOut.writeShort(18);
                // Min Supported API Version
                dOut.writeShort(0);
                // Max Supported API Version
                dOut.writeShort(4);
                // Tag Buffer
                dOut.writeByte(tagBuffer);

            // API Version #3
                // API Key
                dOut.writeShort(requestApiKey);
                // Min Supported API Version
                dOut.writeShort(0);
                // Max Supported API Version
                dOut.writeShort(4);
                // Tag Buffer
                dOut.writeByte(tagBuffer);

            // API Version #4
                dOut.writeShort(requestApiKey);
                // Min Supported API Version
                dOut.writeShort(0);
                // Max Supported API Version
                dOut.writeShort(4);
                // Tag Buffer
                dOut.writeByte(tagBuffer);
            
            // Throttle Time
            dOut.writeInt(0);

            // Tag Buffer
            dOut.writeByte(tagBuffer);

            OutputStream outputStream = clientSocket.getOutputStream();
            DataOutputStream dOutStream = new DataOutputStream(outputStream);
            // Get response message size
            dOutStream.writeInt(byteArrBodyRes.size() + 4);
            dOutStream.writeInt(correlationID);
            dOutStream.write(byteArrBodyRes.toByteArray());


    } catch (IOException e) {
      	System.out.println("IOException: " + e.getMessage());
    } finally {
		try {
			if (clientSocket != null) {
				clientSocket.close();
			}
		} catch (IOException e) {
			System.out.println("IOException: " + e.getMessage());
		}
    }
  }
}
