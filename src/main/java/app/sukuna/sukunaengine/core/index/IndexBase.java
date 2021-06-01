package app.sukuna.sukunaengine.core.index;

import java.util.Map;

public abstract class IndexBase {
    protected Map<String, Long> index;

    public abstract void initialize(String indexName);
    public abstract boolean isInitialized();
    public abstract long getOffset(String key);
    public abstract void upsertOffset(String key, long offset);
    public abstract void deleteOffset(String key);
    public abstract String getHighestKeyLowerThan(String key);
    public abstract String getLowestKeyHigherThan(String key);
    public abstract String[] getKeysOrdered();
}
