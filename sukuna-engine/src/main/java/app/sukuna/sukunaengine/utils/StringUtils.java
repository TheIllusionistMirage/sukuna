package app.sukuna.sukunaengine.utils;

public class StringUtils {
    public static byte[] stringToBinary(String value) {
        if (value == null) {
            throw new IllegalArgumentException("A valid string value must be specified, got null");
        }
        return value.getBytes();
    }

    public static String binaryToString(byte[] value) {
        if (value == null || value.length == 0) {
            throw new IllegalArgumentException("A valid byte array must be specified, got null or a byte array with length 0");
        }
        return new String(value);
    }
}
