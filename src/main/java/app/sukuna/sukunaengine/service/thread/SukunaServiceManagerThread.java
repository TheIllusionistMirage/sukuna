package app.sukuna.sukunaengine.service.thread;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.Configuration;
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

    // temporary for debugging
    private boolean compactionRanOnce = false;

    public SukunaServiceManagerThread(ActiveMemtables activeMemtables, ActiveSSTables activeSSTables) {
        this.running = new AtomicBoolean(false);
        this.activeMemtables = activeMemtables;
        this.activeSSTables = activeSSTables;
        this.lastCompactionTime = LocalDateTime.now();
    }

    @Override
    public void run() {
        this.running.set(true);
        
        while (this.running.get()) {
            // For debugging
            try {
                Thread.sleep(30 * 1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                logger.warn("SukunaServiceManagerThread: Sleep interrupted.");
                e.printStackTrace();
            }

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

            if (this.shouldStartNextCompaction() && !this.compactionRanOnce) {
                this.compactionRanOnce = true;
                this.lastCompactionTime = LocalDateTime.now();
                this.activeSSTablesCompactorThread = new ActiveSSTablesCompactorThread(this.activeSSTables);
                this.activeSSTablesCompactorThread.start();
            }
        }
    }

    public void stopManager() {
        this.running.set(false);
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
        // if (this.lastCompactionTime == null) {
        //     return false;
        // }
        Duration timeSinceLastCompaction = Duration.between(this.lastCompactionTime, LocalDateTime.now());
        return this.activeSSTables.getActiveSSTables().size() >= Configuration.MaxSimultaneousSSTablesAllowed
            && (this.lastCompactionTime == null || (timeSinceLastCompaction.toMinutes() >= Configuration.MinIntervalBetweenConsecutiveCompactions));
    }

    public void stopServiceManager() {
        if (this.memtableToSSTablePersistenceThread != null && this.memtableToSSTablePersistenceThread.isAlive()) {
            this.memtableToSSTablePersistenceThread.interrupt();
        }

        if (this.activeSSTablesCompactorThread != null && this.activeSSTablesCompactorThread.isAlive()) {
            this.activeSSTablesCompactorThread.interrupt();
        }
    }
}
