package app.sukuna.sukunaengine.core.memtable;

import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import app.sukuna.sukunaengine.core.Configuration;

public class Memtable extends MemtableBase {
    private final TreeMap<String, String> memtable = new TreeMap<>();
    private int currentMemtableSize;

    // Locks
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();
    
    public Memtable() {
        super(Configuration.MaxMemtableSizeBeforeSegmentation);
        this.currentMemtableSize = 0;
    }

    @Override
    public void upsert(String key, String value) {
        this.writeLock.lock();

        try {
            if (this.memtable.containsKey(key)) {
                this.currentMemtableSize -= this.memtable.get(key).length();
            }

            this.memtable.put(key, value);
            this.currentMemtableSize += this.memtable.get(key).length();
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public String read(String key) {
        this.readLock.lock();
        try {
            return this.memtable.get(key);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void evict(String key) {
        this.writeLock.lock();

        try {
            if (this.memtable.containsKey(key)) {
                this.currentMemtableSize -= this.memtable.get(key).length();
            }

            this.memtable.remove(key);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean isFull() {
        this.readLock.lock();

        try {
            return this.currentMemtableSize >= this.maxAllowedCapacity;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public String[] getSortedKeys() {
        this.readLock.lock();

        try {
            return this.memtable.keySet().toArray(new String[0]);
        } finally {
            this.readLock.lock();
        }
    }
}
