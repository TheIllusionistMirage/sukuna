package app.sukuna.sukunaengine.core.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.index.InMemoryIndex;
import app.sukuna.sukunaengine.utils.ErrorHandlingUtils;
import app.sukuna.sukunaengine.utils.StringUtils;

public class AppendOnlyLog extends LogBase {
    private static final Logger logger = LoggerFactory.getLogger(AppendOnlyLog.class);
    
    public AppendOnlyLog(String name) {
        super(name, new InMemoryIndex("current-memtable-crash-recovery-log"));
    }

    // @Override
    // public String read(String key) {
        
    // }

    @Override
    public void write(String key, String value) {
        byte[] binary = StringUtils.stringToBinary(key + ":" + value + '\n');

        try {
            this.file.write(binary);
            logger.debug("Wrote value \"{}\" to output file stream: {}", value, this.name);
        } catch (Exception exception) {
            String errorMsg = "Error occurred while writing value \"" + value + "\" output file stream: " + this.name;
            logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(errorMsg, exception));
        }

        this.index.upsertOffset(key, binary.length);
    }

    @Override
    public void close() {
        try {
            this.file.close();
            logger.debug("Closed output file stream: {}", this.name);
        } catch (Exception exception) {
            String errorMsg = "Error occurred while closing output file stream: " + this.name;
            logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(errorMsg, exception));
        }
    }
}
