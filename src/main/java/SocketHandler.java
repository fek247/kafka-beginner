import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class SocketHandler extends Thread {
    private Socket socket;

    public SocketHandler(Socket socket)
    {
        this.socket = socket;
    }

    public void run()
    {
        try {
            InputStream inputStream = socket.getInputStream();
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            OutputStream outputStream = socket.getOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
            ApiVersion apiVersion = new ApiVersion(dataInputStream, dataOutputStream);

            while ((dataInputStream.read()) != -1) {
                // Skip next three byte belong to message size
                dataInputStream.skip(3);
                apiVersion.read();
                apiVersion.write();
            }
        } catch (IOException e) {
            System.out.println("Local address: " + socket.getLocalAddress());
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
