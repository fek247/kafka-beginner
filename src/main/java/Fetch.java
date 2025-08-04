
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

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
        try {
            // Skip last tag buffer, already set above
            dataInputStream.skip(1);
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    @Override
    public void write() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'write'");
    }
    
}
