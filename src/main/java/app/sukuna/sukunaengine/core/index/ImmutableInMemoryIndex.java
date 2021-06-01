package app.sukuna.sukunaengine.core.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImmutableInMemoryIndex extends InMemoryIndex {
    private final Logger logger = LoggerFactory.getLogger(ImmutableInMemoryIndex.class);

    public ImmutableInMemoryIndex() {
        //this.index = new TreeMap<String, Integer>();
        
        logger.debug("DebugId", "Created a new in memory index ({})", InMemoryIndex.class.getName());
    }

    // @Override
    // public void initialize(String[] keys, int[] values) {
        
    // }

    @Override
    public void upsertOffset(String key, int offset) {
        String errorMsg = "Inserting/updating the offset for a key not allowed in this index type";
        logger.error(errorMsg);
        throw new UnsupportedOperationException(errorMsg);
    }

    @Override
    public void deleteOffset(String key) {
        throw new UnsupportedOperationException("Deleting a key not allowed in this index type");
    }

    // @Override
    // public String getHighestKeyLowerThan(String key) {
    //     return ((TreeMap<String, Integer>) this.index).lowerKey(key);
    // }

    // @Override
    // public String getLowestKeyHigherThan(String key) {
    //     return ((TreeMap<String, Integer>) this.index).higherKey(key);
    // }
}
