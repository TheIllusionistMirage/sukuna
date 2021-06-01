package app.sukuna.sukunaengine.core.index;

import java.util.Map;

public abstract class IndexBase {
    protected Map<String, Integer> index;

    public abstract void initialize(String[] keys, int[] values);
    public abstract boolean isInitialized();
    public abstract int getOffset(String key);
    public abstract void upsertOffset(String key, int offset);
    public abstract void deleteOffset(String key);
    public abstract String getHighestKeyLowerThan(String key);
    public abstract String getLowestKeyHigherThan(String key);
}
