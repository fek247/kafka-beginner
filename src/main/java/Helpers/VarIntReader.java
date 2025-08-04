package Helpers;
import java.io.DataInputStream;
import java.io.IOException;

public class VarIntReader {

    /**
     * Giải mã một VarInt không dấu (unsigned).
     * Phù hợp với các trường như độ dài (length) hoặc kích thước (size) không thể âm.
     * @param dataInputStream Luồng dữ liệu đầu vào.
     * @return Giá trị int đã giải mã.
     */
    public static int readUnsignedVarInt(DataInputStream dataInputStream) throws IOException {
        int value = 0;
        int i = 0;
        int b;

        while ((b = dataInputStream.read()) != -1) {
            value |= (b & 0x7F) << i;
            i += 7;

            if ((b & 0x80) == 0) {
                return value; // Không cần Zigzag, chỉ trả về giá trị đã tích lũy
            }

            if (i > 35) { // Quá giới hạn VarInt 32-bit
                throw new IOException("Unsigned VarInt is too large.");
            }
        }
        throw new IOException("Unexpected end of stream while reading Unsigned VarInt.");
    }
    
    /**
     * Giải mã một VarInt có dấu (signed) bằng Zigzag encoding.
     * Phù hợp với các trường có thể có giá trị âm.
     * @param dataInputStream Luồng dữ liệu đầu vào.
     * @return Giá trị int đã giải mã (có dấu).
     */
    public static int readSignedVarInt(DataInputStream dataInputStream) throws IOException {
        int value = 0;
        int i = 0;
        int b;

        while ((b = dataInputStream.read()) != -1) {
            value |= (b & 0x7F) << i;
            i += 7;

            if ((b & 0x80) == 0) {
                // Áp dụng công thức giải mã Zigzag đầy đủ
                return (value >>> 1) ^ -(value & 1);
            }

            if (i > 35) { // Quá giới hạn VarInt 32-bit
                throw new IOException("Signed VarInt is too large.");
            }
        }
        throw new IOException("Unexpected end of stream while reading Signed VarInt.");
    }
}