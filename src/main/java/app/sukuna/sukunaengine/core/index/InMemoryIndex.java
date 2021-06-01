package app.sukuna.sukunaengine.core.index;

import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.Configuration;

public class InMemoryIndex extends IndexBase {
    private final Logger logger = LoggerFactory.getLogger(InMemoryIndex.class);

    public InMemoryIndex() {
        //this.index = new HashMap<>();
        
        logger.debug("DebugId", "Created a new in memory index ({})", InMemoryIndex.class.getName());
    }

    @Override
    public void initialize(String[] keys, int[] values) {
        this.index = new TreeMap<String, Integer>();

        if (keys != null && values != null) {
            if (keys.length != values.length) {
                String errorMsg = "The number of keys and the values should be equal when initializing an index, got key list size: " + keys.length + " and value list size: " + values.length;
                logger.error(errorMsg);
                throw new IllegalArgumentException(errorMsg);
            }
            
            for (int i = 0; i < values.length; i++) {
                this.index.put(keys[i], values[i]);
            }
        }
    }

    @Override
    public boolean isInitialized() {
        return this.index.size() > 0;
    }

    @Override
    public int getOffset(String key) {
        Integer offset = this.index.get(key);
        return offset == null ? Configuration.InvalidIndexOffset : offset.intValue();
    }

    @Override
    public void upsertOffset(String key, int offset) {
        this.index.put(key, offset);
        
        logger.debug("DebugId", "Upserted Key-Value pair into the index - {}:{}", key, offset);
    }

    @Override
    public void deleteOffset(String key) {
        this.index.remove(key);
        
        logger.debug("DebugId", "Removed key {} from the index", key);
    }

    @Override
    public String getHighestKeyLowerThan(String key) {
        return ((TreeMap<String, Integer>) this.index).lowerKey(key);
    }

    @Override
    public String getLowestKeyHigherThan(String key) {
        return ((TreeMap<String, Integer>) this.index).higherKey(key);
    }
}
