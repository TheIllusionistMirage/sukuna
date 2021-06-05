package app.sukuna.sukunaengine.utils;

import java.io.File;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.Configuration;
import app.sukuna.sukunaengine.core.index.InMemoryIndex;

public class IndexUtils {
    private static final Logger logger = LoggerFactory.getLogger(IndexUtils.class);

    public static void persistIndexForSegment(String segmentName, InMemoryIndex index) {
        String indexFileForSegment = "index_" + segmentName;
        try {
            File f = new File(indexFileForSegment);
            
            if (f.exists()) {
                logger.error("Unable to open output file stream: {}, file already exists", indexFileForSegment);
            } else {
                RandomAccessFile indexFile = new RandomAccessFile(indexFileForSegment, "rw");

                String[] orderedKeys = index.getKeysOrdered();

                for (String key : orderedKeys) {
                    long offset = index.getOffset(key);
                    byte keyLength = (byte) key.length();
                    short offsetLength = 8; // bytes, size of long
                    short recordLength = (short) (keyLength + offsetLength + 
                        Configuration.SegmentRecordLengthDescriptorSize + 
                        Configuration.SegmentKeyLengthDescriptorSize);
                    
                    indexFile.writeShort(recordLength);
                    indexFile.writeByte(keyLength);
                    indexFile.write(StringUtils.stringToBinary(key), 0, key.length());
                    indexFile.writeLong(offset);
                }

                indexFile.close();
            }
        } catch (Exception exception) {
            String errorMsg = "Error occurred while opening/writing to output file stream: " + indexFileForSegment;
            logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(errorMsg, exception));
        }
    }
}
