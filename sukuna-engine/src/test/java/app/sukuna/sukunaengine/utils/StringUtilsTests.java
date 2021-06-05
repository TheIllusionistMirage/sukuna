package app.sukuna.sukunaengine.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class StringUtilsTests {
    @Test
    public void stringToBinary_whenInvokedWithValidString_shouldReturnValidBytes() {
        // Setup
        String str = "Hello, World!";
        byte[] expectedBytes = new byte[] { 72, 101, 108, 108, 111, 44, 32, 87, 111, 114, 108, 100, 33 };

        // Act
        byte[] actualBytes = StringUtils.stringToBinary(str);

        // Assert
        for (int i = 0; i < str.length(); ++i) {
            assertEquals(expectedBytes[i], actualBytes[i]);
        }
    }

    @Test
    public void stringToBinary_whenInvokedWithInvalidString_shouldThrowIllegalArgumentException() {
        // Setup
        String str = null;

        // Act
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            StringUtils.stringToBinary(str);
        });

        // Assert
        String expectedMessage = "A valid string value must be specified, got null";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    public void binaryToString_whenInvokedWithValidByteArray_shouldReturnValidString() {
        // Setup
        String expectedValue = "Hello, World!";
        byte[] bytes = new byte[] { 72, 101, 108, 108, 111, 44, 32, 87, 111, 114, 108, 100, 33 };

        // Act
        String actualValue = StringUtils.binaryToString(bytes);

        // Assert
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void binaryToString_whenInvokedWithInvalidByteArrayWithNullValue_shouldThrowIllegalArgumentException() {
        // Setup
        byte[] bytes = null;

        // Act
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            StringUtils.binaryToString(bytes);
        });

        // Assert
        String expectedMessage = "A valid byte array must be specified, got null or a byte array with length 0";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    public void binaryToString_whenInvokedWithInvalidByteArrayWithNoValues_shouldThrowIllegalArgumentException() {
        // Setup
        byte[] bytes = new byte[] {};

        // Act
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            StringUtils.binaryToString(bytes);
        });

        // Assert
        String expectedMessage = "A valid byte array must be specified, got null or a byte array with length 0";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage));
    }
}
