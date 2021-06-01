package app.sukuna.sukunaengine.core.segment.generator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import app.sukuna.sukunaengine.core.memtable.Memtable;
import app.sukuna.sukunaengine.utils.ErrorHandlingUtils;
import app.sukuna.sukunaengine.utils.StringUtils;

public class MemtableSegmentGenerator implements ISegmentGenerator {
    private static final Logger logger = LoggerFactory.getLogger(MemtableSegmentGenerator.class);

    public void fromMemtable(String segmentName, Memtable memtable) {
        try {
            OutputStream file;
            File f = new File(segmentName);
            
            if (f.exists()) {
                logger.error("Unable to open output file stream: {}, file already exists", segmentName);
            } else {
                file = new FileOutputStream(segmentName);

                for (var key : memtable.getSortedKeys()) {
                    String segmentRecord = key + ":" + memtable.read(key) + "\n";
                    file.write(StringUtils.stringToBinary(segmentRecord));
                }

                file.close();
            }
        } catch (Exception exception) {
            String errorMsg = "Error occurred while opening output file stream: " + segmentName;
            logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(errorMsg, exception));
        }
    }
}
