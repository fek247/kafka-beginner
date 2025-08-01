import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ApiVersion {
    private short apiKey;

    private short apiVersion;

    private int correlationId;

    private short headerLength;

    private byte[] headerContent = new byte[9];

    private byte tagBuffer;

    private byte bodyLength;

    private byte[] bodyContent = new byte[9];

    private int softwareVersion;

    private DataInputStream dataInputstream;

    private DataOutputStream dataOutputStream;

    public ApiVersion(DataInputStream inputStream)
    {
        this.dataInputstream = inputStream;
    }

    public ApiVersion(DataOutputStream outputStream)
    {
        this.dataOutputStream = outputStream;
    }

    public ApiVersion(DataInputStream inputStream, DataOutputStream outputStream)
    {
        this.dataInputstream = inputStream;
        this.dataOutputStream = outputStream;
    }

    public void read()
    {
        if (dataInputstream == null) {
            return;
        }
        try {
            setApiKey(dataInputstream.readShort());
            setApiVersion(dataInputstream.readShort());
            setCorrelationId(dataInputstream.readInt());
            setHeaderLength(dataInputstream.readShort());
            byte[] headerContent = new byte[9];
            dataInputstream.read(headerContent);
            setHeaderContent(headerContent);
            setTagBuffer(dataInputstream.readByte());
            setBodyLength(dataInputstream.readByte());
            byte[] bodyContent = new byte[9];
            dataInputstream.read(bodyContent);
            setBodyContent(bodyContent);
            setSoftwareVersion(dataInputstream.readInt());
            // Skip last tag buffer, already set above
            dataInputstream.skip(1);
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    public void write()
    {
        if (dataOutputStream == null) {
            return;
        }

        ByteArrayOutputStream byteArrBodyRes = this.responseBody();
        try {
            this.dataOutputStream.writeInt(byteArrBodyRes.size() + 4);
            this.dataOutputStream.writeInt(this.getCorrelationId());
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
            int code = (this.getApiVersion() < 0 || this.getApiVersion() > 4) ? 35 : 0;
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
                    dOut.writeByte(this.getTagBuffer());

                // API Version #2
                    // API Key
                    dOut.writeShort(this.getApiKey());
                    // Min Supported API Version
                    dOut.writeShort(0);
                    // Max Supported API Version
                    dOut.writeShort(4);
                    // Tag Buffer
                    dOut.writeByte(this.getTagBuffer());

                // API Version #3
                    // API Key
                    dOut.writeShort(75);
                    // Min Supported API Version
                    dOut.writeShort(0);
                    // Max Supported API Version
                    dOut.writeShort(4);
                    // Tag Buffer
                    dOut.writeByte(this.getTagBuffer());
            // Throttle Time
            dOut.writeInt(0);

            // Tag Buffer
            dOut.writeByte(this.getTagBuffer());
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }

        return byteArr;
    }

    public void setDataInputStream(DataInputStream dataInputStream)
    {
        this.dataInputstream = dataInputStream;
    }

    public void setDataOutputStream(DataOutputStream dataOutputStream)
    {
        this.dataOutputStream = dataOutputStream;
    }

    private void setApiKey(short apiKey)
    {
        this.apiKey = apiKey;
    }

    private void setApiVersion(short apiVersion)
    {
        this.apiVersion = apiVersion;
    }

    private void setCorrelationId(int correlationId)
    {
        this.correlationId = correlationId;
    }

    private void setHeaderLength(short headerLength)
    {
        this.headerLength = headerLength;
    }

    private void setHeaderContent(byte[] headerContent)
    {
        this.headerContent = headerContent;
    }

    private void setTagBuffer(byte tagBuffer)
    {
        this.tagBuffer = tagBuffer;
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

    public short getApiKey()
    {
        return this.apiKey;
    }

    public short getApiVersion()
    {
        return this.apiVersion;
    }

    public int getCorrelationId()
    {
        return this.correlationId;
    }

    public short getHeaderLength()
    {
        return this.headerLength;
    }

    public byte[] getHeaderContent()
    {
        return this.headerContent;
    }

    public byte getTagBuffer()
    {
        return this.tagBuffer;
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