package app.sukuna.sukunaengine.core.index;

import java.io.RandomAccessFile;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.Configuration;
import app.sukuna.sukunaengine.utils.ErrorHandlingUtils;
import app.sukuna.sukunaengine.utils.FileUtils;

public class InMemoryIndex extends IndexBase {
    private final Logger logger = LoggerFactory.getLogger(InMemoryIndex.class);

    public InMemoryIndex(String name) {
        super(name);

        //this.index = new HashMap<>();
        this.index = new TreeMap<String, Long>();
        
        logger.debug("Created a new in memory index ({})", InMemoryIndex.class.getName());
    }

    @Override
    public void initialize(String segmentName) {
        String indexFileName = "index_" + segmentName;

        logger.info("Initializing index from index file \"" + indexFileName + "\" for segment \"" + segmentName + "\"...");

        // Check if index file for segment exists
        if (!FileUtils.fileExists(indexFileName)) {
            // error
            logger.error("Index file for \"" + segmentName + "\" does not exist");
        }

        try {
            RandomAccessFile indexFile = new RandomAccessFile(indexFileName, "r");

            // Read all index records
            long indexFileLength = indexFile.length();
            long totalBytesRead = 0;

            this.index = new TreeMap<String, Long>();

            while (totalBytesRead < indexFileLength) {
                int recordLength = (int) indexFile.readShort();
                int keyLength = (int) indexFile.readByte();
                
                // Read the key
                byte[] keyByteArray = new byte[keyLength];
                indexFile.read(keyByteArray, 0, keyLength);
                String recordKey = new String(keyByteArray);
                long offset = indexFile.readLong();

                this.index.put(recordKey, offset);

                totalBytesRead += recordLength;
            }

            indexFile.close();
            logger.info("Successfully read index info from index file \"" + indexFileName + "\"");
        } catch (Exception exception) {
            String errorMsg = "Error occurred while opening input file stream: " + indexFileName;
            logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(errorMsg, exception));
        }
    }

    @Override
    public boolean isInitialized() {
        return this.index.size() > 0;
    }

    @Override
    public long getOffset(String key) {
        Long offset = this.index.get(key);
        return offset == null ? Configuration.InvalidIndexOffset : offset.longValue();
    }

    @Override
    public void upsertOffset(String key, long offset) {
        this.index.put(key, offset);
        
        logger.debug("Upserted Key-Value pair into the index - {}:{}", key, offset);
    }

    @Override
    public void deleteOffset(String key) {
        this.index.remove(key);
        
        logger.debug("Removed key {} from the index", key);
    }

    @Override
    public String getHighestKeyLowerThan(String key) {
        return ((TreeMap<String, Long>) this.index).lowerKey(key);
    }

    @Override
    public String getLowestKeyHigherThan(String key) {
        return ((TreeMap<String, Long>) this.index).higherKey(key);
    }

    @Override
    public String[] getKeysOrdered() {
        return this.index.keySet().toArray(new String[0]);
    }
}
