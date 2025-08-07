package Helpers;
import java.io.DataInputStream;
import java.io.IOException;

public class VarIntReader {
    public static int readUnsignedVarInt(DataInputStream dataInputStream) throws IOException {
        int value = 0;
        int i = 0;
        int b;

        while ((b = dataInputStream.read()) != -1) {
            value |= (b & 0x7F) << i;
            i += 7;

            if ((b & 0x80) == 0) {
                return value;
            }

            if (i > 35) {
                throw new IOException("Unsigned VarInt is too large.");
            }
        }
        throw new IOException("Unexpected end of stream while reading Unsigned VarInt.");
    }

    public static int readSignedVarInt(DataInputStream dataInputStream) throws IOException {
        int value = 0;
        int i = 0;
        int b;

        while ((b = dataInputStream.read()) != -1) {
            value |= (b & 0x7F) << i;
            i += 7;

            if ((b & 0x80) == 0) {
                return (value >>> 1) ^ -(value & 1);
            }

            if (i > 35) {
                throw new IOException("Signed VarInt is too large.");
            }
        }
        throw new IOException("Unexpected end of stream while reading Signed VarInt.");
    }
}