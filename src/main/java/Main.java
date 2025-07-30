import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

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

        short length = dIn.readShort();
        System.out.println("length: " + length);

        // Write
        OutputStream outputStream = clientSocket.getOutputStream();
        DataOutputStream dOut = new DataOutputStream(outputStream);
        dOut.writeInt(messageSize);
        dOut.writeInt(correlationID);
        int code = (requestApiVersion < 0 || requestApiVersion > 4) ? 35 : 0;
        dOut.writeShort(code);
        dOut.write(length);
        dOut.writeShort(requestApiKey);
        dOut.writeShort(4);
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
