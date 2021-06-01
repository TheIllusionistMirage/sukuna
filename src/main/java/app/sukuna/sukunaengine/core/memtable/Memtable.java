package app.sukuna.sukunaengine.core.memtable;

import java.util.TreeMap;

import app.sukuna.sukunaengine.core.Configuration;

public class Memtable extends MemtableBase {
    private final TreeMap<String, String> memtable = new TreeMap<>();
    private int currentMemtableSize;
    
    public Memtable() {
        super(Configuration.MaxMemtableSizeBeforeSegmentation);
        this.currentMemtableSize = 0;
    }

    @Override
    public void upsert(String key, String value) {
        if (this.memtable.containsKey(key)) {
            this.currentMemtableSize -= this.memtable.get(key).length();
        }

        this.memtable.put(key, value);
        this.currentMemtableSize += this.memtable.get(key).length();
    }

    @Override
    public String read(String key) {
        return this.memtable.get(key);
    }

    @Override
    public void evict(String key) {
        if (this.memtable.containsKey(key)) {
            this.currentMemtableSize -= this.memtable.get(key).length();
        }

        this.memtable.remove(key);
    }

    @Override
    public boolean isFull() {
        return this.currentMemtableSize >= this.maxAllowedCapacity;
    }

    @Override
    public String[] getSortedKeys() {
        return this.memtable.keySet().toArray(new String[0]);
    }
}
