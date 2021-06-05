package app.sukuna.sukunaengine.core.log;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.index.IndexBase;
import app.sukuna.sukunaengine.utils.ErrorHandlingUtils;

public abstract class LogBase {
    public final String name;
    public final int maxLogSize;
    protected IndexBase index;
    protected OutputStream file;
    private static final Logger logger = LoggerFactory.getLogger(LogBase.class);

    public LogBase(String name, int maxLogSize, IndexBase index) {
        this.name = name;
        this.maxLogSize = maxLogSize;
        this.index = index;

        try {
            File f = new File(this.name);
            if (f.exists()) {
                logger.error("Unable to open output file stream: {}, file already exists", this.name);
                this.file = null;
            } else {
                this.file = new FileOutputStream(name);
            }
        } catch (Exception exception) {
            String errorMsg = "Error occurred while opening output file stream: " + this.name;
            logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(errorMsg, exception));
            this.file = null;
        }
    }

    @Override
    public void finalize() {
        try {
            this.close();
            logger.debug("Closed output file stream: {}", this.name);
        } catch (Exception exception) {
            String errorMsg = "Error occurred while closing output file stream: " + this.name;
            logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(errorMsg, exception));
        }
    }

    // public abstract String read(String key);

    public abstract void write(String key, String value);

    // public abstract void upsertKeyToIndex(String key, int offset);
    // public abstract void deleteKeyFromIndex(String key);
    public abstract void close();
}
