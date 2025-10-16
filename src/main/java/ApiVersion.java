import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import Constant.ApiKey;
import Helpers.VarIntReader;

public class ApiVersion extends BaseApi {
    public static int UNSUPPORTED_VERSION = 35;

    private byte bodyLength;

    private byte[] bodyContent;

    private int softwareVersionLength;

    private byte[] softwareVersionContent;

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
            byte[] bodyContent = new byte[this.getBodyLength() - 1];
            dataInputStream.read(bodyContent);
            setBodyContent(bodyContent);
            int softwareVersionLength = VarIntReader.readUnsignedVarInt(dataInputStream);
            setSoftwareVersionLength(softwareVersionLength);
            byte[] softwareVersionContent = new byte[this.softwareVersionLength - 1];
            dataInputStream.read(softwareVersionContent);
            setSoftwareVersionContent(softwareVersionContent);
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
                dOut.writeByte(5);
                // API Version #1
                    // API Key
                    dOut.writeShort(ApiKey.Fetch);
                    // Min Supported API Version
                    dOut.writeShort(0);
                    // Max Supported API Version
                    dOut.writeShort(17);
                    // Tag Buffer
                    dOut.writeByte(this.header.getTagBuffer());

                // API Version #2
                    // API Key
                    dOut.writeShort(ApiKey.ApiVersions);
                    // Min Supported API Version
                    dOut.writeShort(0);
                    // Max Supported API Version
                    dOut.writeShort(4);
                    // Tag Buffer
                    dOut.writeByte(this.header.getTagBuffer());

                // API Version #3
                    // API Key
                    dOut.writeShort(ApiKey.DescribeTopicPartitions);
                    // Min Supported API Version
                    dOut.writeShort(0);
                    // Max Supported API Version
                    dOut.writeShort(4);
                    // Tag Buffer
                    dOut.writeByte(this.header.getTagBuffer());

                // API Version #4
                    // API Key
                    dOut.writeShort(ApiKey.Produce);
                    // Min Supported API Version
                    dOut.writeShort(0);
                    // Max Supported API Version
                    dOut.writeShort(11);
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

    public byte getBodyLength()
    {
        return this.bodyLength;
    }

    public byte[] getBodyContent()
    {
        return this.bodyContent;
    }

    public int getSoftwareVersionLength() {
        return softwareVersionLength;
    }

    public void setSoftwareVersionLength(int softwareVersionLength) {
        this.softwareVersionLength = softwareVersionLength;
    }

    public byte[] getSoftwareVersionContent() {
        return softwareVersionContent;
    }

    public void setSoftwareVersionContent(byte[] softwareVersionContent) {
        this.softwareVersionContent = softwareVersionContent;
    }
}