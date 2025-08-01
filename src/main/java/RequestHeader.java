public class RequestHeader {
    private short apiKey;

    private short apiVersion;

    private int correlationId;

    private short clientLength;

    private byte[] clientContent;

    private byte tagBuffer;

    protected void setApiKey(short apiKey)
    {
        this.apiKey = apiKey;
    }

    protected void setApiVersion(short apiVersion)
    {
        this.apiVersion = apiVersion;
    }

    protected void setCorrelationId(int correlationId)
    {
        this.correlationId = correlationId;
    }

    protected void setClientLength(short clientLength)
    {
        this.clientLength = clientLength;
    }

    protected void setClientContent(byte[] clientContent)
    {
        this.clientContent = clientContent;
    }

    protected void setTagBuffer(byte tagBuffer)
    {
        this.tagBuffer = tagBuffer;
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
        return this.clientLength;
    }

    public byte[] getHeaderContent()
    {
        return this.clientContent;
    }

    public byte getTagBuffer()
    {
        return this.tagBuffer;
    }
}
