package app.sukuna.sukunaengine.core.memtable;

public abstract class MemtableBase {
    protected final long maxAllowedCapacity;

    public MemtableBase(long memtableCapacity) {
        this.maxAllowedCapacity = memtableCapacity;
    }

    public abstract void upsert(String Stringey, String value);
    public abstract String read(String key);
    public abstract void evict(String key);
    public abstract boolean isFull();
    public abstract String[] getSortedKeys();
}
