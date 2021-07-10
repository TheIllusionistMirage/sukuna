package app.sukuna.sukunaengine.service.thread;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.configuration.Configuration;
import app.sukuna.sukunaengine.core.memtable.Memtable;
import app.sukuna.sukunaengine.service.ActiveMemtables;
import app.sukuna.sukunaengine.service.ActiveSSTables;

public class SukunaServiceManagerThread extends Thread {
    private AtomicBoolean running;
    private final ActiveMemtables activeMemtables;
    private final ActiveSSTables activeSSTables;
    private LocalDateTime lastCompactionTime;
    private MemtableToSSTablePersistenceThread memtableToSSTablePersistenceThread;
    private ActiveSSTablesCompactorThread activeSSTablesCompactorThread;
    private final static Logger logger = LoggerFactory.getLogger(SukunaServiceManagerThread.class);

    public SukunaServiceManagerThread(ActiveMemtables activeMemtables, ActiveSSTables activeSSTables) {
        this.setName("SukunaServiceManagerThread");
        this.running = new AtomicBoolean(false);
        this.activeMemtables = activeMemtables;
        this.activeSSTables = activeSSTables;
        this.lastCompactionTime = LocalDateTime.now();
    }

    @Override
    public void run() {
        this.running.set(true);
        
        while (this.running.get()) {
            // TODO: Unsure if current in the implementation a simultaneous memtable to SSTable persistence and a compaction thead can affect each other

            Memtable currentMemtable = this.activeMemtables.getCurrentMemtableReadWriteMode();
            
            // If current memtable is full, create a new memtable, mark
            // that as the current memtable and put the full memtable in
            // the list of full memtables that will be persisted to SSTables
            if (currentMemtable.isFull()) {
                this.updateActiveMemtables(currentMemtable);
                
                this.memtableToSSTablePersistenceThread = new MemtableToSSTablePersistenceThread(currentMemtable, this.activeMemtables, this.activeSSTables);
                this.memtableToSSTablePersistenceThread.start();
            }

            if (this.shouldStartNextCompaction()) {
                this.lastCompactionTime = LocalDateTime.now();
                this.activeSSTablesCompactorThread = new ActiveSSTablesCompactorThread(this.activeSSTables);
                this.activeSSTablesCompactorThread.start();
            }
        }
    }

    private void updateActiveMemtables(Memtable currentMemtable) {
        Memtable emptyMemtable = new Memtable();

        // NOTE: Completely cloning memtables is highly sub-optimal!
        // Hence just the list is newly created, but elements are reused
        List<Memtable> fullMemtables = this.activeMemtables.getFullMemtablesReadWriteMode();
        
        List<Memtable> updatedFullMemtables = new ArrayList<>(fullMemtables.size() + 1);
        // Add the current memtable at the beginning since more recent the 
        // memtable, the earlier it is checked when handling read operations
        updatedFullMemtables.add(currentMemtable);

        for (Memtable fullMemtable : fullMemtables) {
            updatedFullMemtables.add(fullMemtable);
        }
        
        this.activeMemtables.updateCurrentMemtable(emptyMemtable);
        this.activeMemtables.updateFullMemtables(updatedFullMemtables);
    }

    private boolean shouldStartNextCompaction() {
        Duration timeSinceLastCompaction = Duration.between(this.lastCompactionTime, LocalDateTime.now());
        return this.activeSSTables.getActiveSSTables().size() >= Configuration.MaxSimultaneousSegmentsAllowed
            && (this.lastCompactionTime == null || (timeSinceLastCompaction.toMinutes() >= Configuration.MinIntervalBetweenConsecutiveCompactionsInMins));
    }

    public void stopServiceManager() {
        this.running.set(false);

        // TODO: Ensure that stopping the Sukuna Engine ensures pending operations to get completed first
        logger.info("Stopping service manager thread...");

        if (this.memtableToSSTablePersistenceThread != null && this.memtableToSSTablePersistenceThread.isAlive()) {
            logger.warn("Memtable to SSTable persistence thread currently active, terminating thread");
            this.memtableToSSTablePersistenceThread.interrupt();
        }

        if (this.activeSSTablesCompactorThread != null && this.activeSSTablesCompactorThread.isAlive()) {
            logger.warn("SSTable compaction thread currently active, terminating thread");
            this.activeSSTablesCompactorThread.interrupt();
        }

        logger.info("Stopped service manager thread [OK]");
        this.interrupt();
    }
}
