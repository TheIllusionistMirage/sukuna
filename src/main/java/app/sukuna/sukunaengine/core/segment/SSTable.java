package app.sukuna.sukunaengine.core.segment;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.Configuration;
import app.sukuna.sukunaengine.core.index.IndexBase;
import app.sukuna.sukunaengine.utils.ErrorHandlingUtils;
import app.sukuna.sukunaengine.utils.FileUtils;

public class SSTable extends SegmentBase {
    private static final Logger logger = LoggerFactory.getLogger(SSTable.class);

    public SSTable() {
        // this.index = new InMemoryIndex();
    }

    @Override
    public void initialize(String segmentName, IndexBase index) {
        logger.info("Initializing SSTable from segment file \"" + segmentName + "\"");

        // Check if segment file exists
        if (!FileUtils.fileExists(segmentName)) {
            // error
            logger.error("Segment file \"" + segmentName + "\" does not exist");
        }

        this.name = segmentName;

        try {
            this.segmentFile = new RandomAccessFile(this.name, "r");
            this.size = this.segmentFile.length();

            logger.info("Successfully initialized SSTable from segment file \"" + segmentName + "\"");
        } catch (Exception exception) {
            String errorMsg = "Error occurred while opening input file stream: " + this.name;
            logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(errorMsg, exception));
        }

        if (index == null || !index.isInitialized()) {
            String errorMsg = "Error occurred while initializing segment: " + this.name + ", invalid index";
            logger.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }
        this.index = index;
    }

    @Override
    public String read(String key) {
        long offset = this.index.getOffset(key);

        // Record not indexed in memory, find it in SSTable
        if (offset == Configuration.InvalidIndexOffset) {
            // Find which block this key belongs to
            
            logger.debug("Searching for key: " + key);
            String highestKeyLower = this.index.getHighestKeyLowerThan(key);

            // Specified key does not exist in this SSTable
            if (highestKeyLower == null) {
                return null;
            }

            try {
                // Check the entire block, record by record to see if the key exists
                offset = this.index.getOffset(highestKeyLower);
                long blockSize = this.segmentFile.length();
                String value = null;

                while (offset < blockSize) {
                    this.segmentFile.seek(offset);

                    // First 2 bytes from the offset containt the length of the record
                    int recordLength = (int) this.segmentFile.readShort();
                    // Next offset will be the beginning of the next record
                    offset += recordLength;

                    // The next recordLength bytes contain the actual record

                    // The next 1 byte describes the length of the key in the record
                    int keyLength = (int) this.segmentFile.readByte();
                    int valueLength = recordLength - keyLength - Configuration.SegmentRecordLengthDescriptorSize - Configuration.SegmentKeyLengthDescriptorSize;

                    // Read the key
                    byte[] keyByteArray = new byte[keyLength];
                    this.segmentFile.read(keyByteArray, 0, keyLength);
                    String recordKey = new String(keyByteArray);

                    if (!recordKey.equals(key)) {
                        continue;
                    }

                    // Read the value
                    byte[] valueByteArray = new byte[valueLength];
                    this.segmentFile.read(valueByteArray, 0, valueLength);

                    value = new String(valueByteArray);
                    break;
                }

                return value;
            } catch (IOException exception) {
                String errorMsg = "Error occurred while trying to determine length of input file stream: " + this.name;
                logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(errorMsg, exception));
            }
        }
        // Record indexed in memory, retrieve it
        else {
            try {
                this.segmentFile.seek(offset);

                // First 2 bytes from the offset containt the length of the record
                int recordLength = (int) this.segmentFile.readShort();

                // The next recordLength bytes contain the actual record

                // The next 1 byte describes the length of the key in the record
                int keyLength = (int) this.segmentFile.readByte();
                int valueLength = recordLength - keyLength - Configuration.SegmentRecordLengthDescriptorSize - Configuration.SegmentKeyLengthDescriptorSize;

                // Read the key
                byte[] keyByteArray = new byte[keyLength];
                this.segmentFile.read(keyByteArray, 0, keyLength);

                // Read the value
                byte[] valueByteArray = new byte[valueLength];
                this.segmentFile.read(valueByteArray, 0, valueLength);

                String value = new String(valueByteArray);
                return value;
            } catch (Exception exception) {
                String errorMsg = "Error occurred while reading input file stream: " + this.name;
                logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(errorMsg, exception));
            }
        }

        return null;
    }

    @Override
    public void close() throws IOException {
        this.segmentFile.close();
        logger.info("Closed input stream for segment file: " + this.name);        
    }

    @Override
    public void finalize() throws IOException {
        this.close();
    }
}
