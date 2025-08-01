import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ApiVersion extends BaseApi {
    public static int UNSUPPORTED_VERSION = 35;

    private byte bodyLength;

    private byte[] bodyContent = new byte[9];

    private int softwareVersion;

    public ApiVersion(DataInputStream inputStream)
    {
        this.dataInputStream = inputStream;
    }

    public ApiVersion(DataOutputStream outputStream)
    {
        this.dataOutputStream = outputStream;
    }

    public ApiVersion(DataInputStream inputStream, DataOutputStream outputStream)
    {
        this.dataInputStream = inputStream;
        this.dataOutputStream = outputStream;
    }

    @Override
    public void read()
    {
        if (dataInputStream == null) {
            return;
        }
        try {
            setBodyLength(dataInputStream.readByte());
            byte[] bodyContent = new byte[9];
            dataInputStream.read(bodyContent);
            setBodyContent(bodyContent);
            setSoftwareVersion(dataInputStream.readInt());
            // Skip last tag buffer, already set above
            dataInputStream.skip(1);
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    @Override
    public void write()
    {
        if (dataOutputStream == null) {
            return;
        }

        ByteArrayOutputStream byteArrBodyRes = this.responseBody();
        try {
            this.dataOutputStream.writeInt(byteArrBodyRes.size() + 4);
            this.dataOutputStream.writeInt(this.header.getCorrelationId());
            this.dataOutputStream.write(byteArrBodyRes.toByteArray());
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    public ByteArrayOutputStream responseBody()
    {
        ByteArrayOutputStream byteArr = new ByteArrayOutputStream();
        DataOutputStream dOut = new DataOutputStream(byteArr);

        try {
            int code = (this.header.getApiVersion() < 0 || this.header.getApiVersion() > 4) ? UNSUPPORTED_VERSION : 0;
            // Error code
            dOut.writeShort(code);
            // API Version Array
                // Array Length
                dOut.writeByte(4);
                // API Version #1
                    // API Key
                    dOut.writeShort(1);
                    // Min Supported API Version
                    dOut.writeShort(0);
                    // Max Supported API Version
                    dOut.writeShort(17);
                    // Tag Buffer
                    dOut.writeByte(this.header.getTagBuffer());

                // API Version #2
                    // API Key
                    dOut.writeShort(this.header.getApiKey());
                    // Min Supported API Version
                    dOut.writeShort(0);
                    // Max Supported API Version
                    dOut.writeShort(4);
                    // Tag Buffer
                    dOut.writeByte(this.header.getTagBuffer());

                // API Version #3
                    // API Key
                    dOut.writeShort(75);
                    // Min Supported API Version
                    dOut.writeShort(0);
                    // Max Supported API Version
                    dOut.writeShort(4);
                    // Tag Buffer
                    dOut.writeByte(this.header.getTagBuffer());
            // Throttle Time
            dOut.writeInt(0);

            // Tag Buffer
            dOut.writeByte(this.header.getTagBuffer());
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }

        return byteArr;
    }

    private void setBodyLength(byte bodyLength)
    {
        this.bodyLength = bodyLength;
    }

    private void setBodyContent(byte[] bodyContent)
    {
        this.bodyContent = bodyContent;
    }

    private void setSoftwareVersion(int softwareVersion)
    {
        this.softwareVersion = softwareVersion;
    }

    public byte getBodyLength()
    {
        return this.bodyLength;
    }

    public byte[] getBodyContent()
    {
        return this.bodyContent;
    }

    public int getSoftwareVersion()
    {
        return this.softwareVersion;
    }
}