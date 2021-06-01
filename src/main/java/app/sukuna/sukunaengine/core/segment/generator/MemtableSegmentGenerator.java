package app.sukuna.sukunaengine.core.segment.generator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.Configuration;
import app.sukuna.sukunaengine.core.index.ImmutableInMemoryIndex;
import app.sukuna.sukunaengine.core.index.InMemoryIndex;
import app.sukuna.sukunaengine.core.index.IndexBase;
import app.sukuna.sukunaengine.core.memtable.Memtable;
import app.sukuna.sukunaengine.utils.ErrorHandlingUtils;
import app.sukuna.sukunaengine.utils.IndexUtils;
import app.sukuna.sukunaengine.utils.StringUtils;

public class MemtableSegmentGenerator implements ISegmentGenerator {
    private static final Logger logger = LoggerFactory.getLogger(MemtableSegmentGenerator.class);

    public ImmutableInMemoryIndex fromMemtable(String segmentName, Memtable memtable) {
        try {
            File f = new File(segmentName);
            
            if (f.exists()) {
                logger.error("Unable to open output file stream: {}, file already exists", segmentName);
            } else {
                RandomAccessFile segmentFile = new RandomAccessFile(segmentName, "rw");
                InMemoryIndex index = new InMemoryIndex();
                // index.initialize(null, null);
                
                // Total bytes written to the segment file so far
                long totalBytesWritten = 0;
                // Total bytes corresponding to the current block written so far
                long blockBytesWritten = 0;

                String[] sortedKeys = memtable.getSortedKeys();

                // The first indexed key will always be the key at the beginning of the SSTable 
                index.upsertOffset(sortedKeys[0], 0);

                for (int i = 0; i < sortedKeys.length; i++) {
                    if (blockBytesWritten >= Configuration.SegmentBlockSize) {
                        index.upsertOffset(sortedKeys[i], totalBytesWritten);
                        blockBytesWritten = 0;
                    }

                    String key = sortedKeys[i];
                    String value = memtable.read(key);
                    byte keyLength = (byte) key.length();
                    short valueLength = (short) value.length();
                    short recordLength = (short) (keyLength + valueLength + 
                        Configuration.SegmentRecordLengthDescriptorSize + 
                        Configuration.SegmentKeyLengthDescriptorSize);
                    
                    segmentFile.writeShort(recordLength);
                    segmentFile.writeByte(keyLength);
                    segmentFile.write(StringUtils.stringToBinary(key), 0, key.length());
                    segmentFile.write(StringUtils.stringToBinary(value), 0, value.length());

                    totalBytesWritten += recordLength;
                    blockBytesWritten += recordLength;
                }

                segmentFile.close();

                // Persist index data for segment too
                IndexUtils.persistIndexForSegment(segmentName, index);

                ImmutableInMemoryIndex immutableInMemoryIndex = new ImmutableInMemoryIndex();
                immutableInMemoryIndex.createFrom(index);
                return immutableInMemoryIndex;
            }
        } catch (Exception exception) {
            String errorMsg = "Error occurred while opening/writing to output file stream: " + segmentName;
            logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(errorMsg, exception));
            return null;
        }

        return null;
    }
}
