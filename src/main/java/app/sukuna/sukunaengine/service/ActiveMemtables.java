package app.sukuna.sukunaengine.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import app.sukuna.sukunaengine.core.memtable.Memtable;

public class ActiveMemtables {
    private Memtable currentMemtable;
    private List<Memtable> fullMemtables;

    // Locks
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();

    public ActiveMemtables() {
        this.currentMemtable = new Memtable();
        this.fullMemtables = new ArrayList<>();
    }

    // Returns the current memtable in read-write mode for use by ReaderWorker/WriterWorker threads 
    public Memtable getCurrentMemtableReadWriteMode() {
        this.readLock.lock();

        try {
            return this.currentMemtable;
        } finally {
            this.readLock.unlock();
        }
    }

    // Returns the full memtables in read-write mode for use by ReaderWorker/WriterWorker threads 
    public List<Memtable> getFullMemtablesReadWriteMode() {
        this.readLock.lock();

        try {
            return this.fullMemtables;
        } finally {
            this.readLock.unlock();
        }
    }

    // Update the current memtable from the "X" thread. 
    // WARNING: The "X" thread should be the only one that invokes this method 
    public void updateCurrentMemtable(Memtable emptyMemtable) {
        this.writeLock.lock();

        try {
            this.currentMemtable = emptyMemtable;
        } finally {
            this.writeLock.unlock();
        }
    }

    // Update the full memtable from the "X" thread
    // WARNING: The "X" thread should be the only one that invokes this method
    public void updateFullMemtables(List<Memtable> fullMemtables) {
        this.writeLock.lock();

        try {
            this.fullMemtables = fullMemtables;
        } finally {
            this.writeLock.unlock();
        }
    }
}
