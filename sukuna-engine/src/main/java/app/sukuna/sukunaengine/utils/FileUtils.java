package app.sukuna.sukunaengine.utils;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils {
    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    public static boolean fileExists(String path) {
        try {
            File file = new File(path);
            if (file.exists()) {
                return true;
            } else {
                return false;
            }
        } catch (Exception exception) {
            String errorMsg = "Error occurred while opening output file stream: " + path;
            logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(errorMsg, exception));
            return false;
        }
    }
}
