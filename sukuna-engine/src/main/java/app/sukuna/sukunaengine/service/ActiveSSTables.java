package app.sukuna.sukunaengine.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveSSTables {
    private List<String> segmentNames;

    // Locks
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();
    private final static Logger logger = LoggerFactory.getLogger(ActiveSSTables.class);

    public ActiveSSTables() {
        this.segmentNames = new ArrayList<>();
    }

    public List<String> getActiveSSTables() {
        this.readLock.lock();

        try {
            return this.segmentNames;
        } finally {
            this.readLock.unlock();
        }
    }

    public void updateActiveSSTables(List<String> activeSSTables) {
        this.writeLock.lock();

        try {
            this.segmentNames = activeSSTables;
        } finally {
            this.writeLock.unlock();
        }
    }
}
