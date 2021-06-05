package app.sukuna.sukunaengine.service.thread;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.memtable.Memtable;
import app.sukuna.sukunaengine.core.segment.generator.ISegmentGenerator;
import app.sukuna.sukunaengine.core.segment.generator.MemtableSegmentGenerator;
import app.sukuna.sukunaengine.service.ActiveMemtables;
import app.sukuna.sukunaengine.service.ActiveSSTables;

public class MemtableToSSTablePersistenceThread extends Thread {
    private Memtable memtable;
    private ActiveMemtables activeMemtables;
    private ActiveSSTables activeSSTables;
    private final static Logger logger = LoggerFactory.getLogger(MemtableToSSTablePersistenceThread.class);

    public MemtableToSSTablePersistenceThread(Memtable memtable, ActiveMemtables activeMemtables, ActiveSSTables activeSSTables) {
        this.memtable = memtable;
        this.activeMemtables = activeMemtables;
        this.activeSSTables = activeSSTables;
    }

    // TODO: Handle interrupt
    @Override
    public void run() {
        logger.info("Memtable to SSTable persistence thread invoked, affected memtable: " + memtable.name);
        String segmentName = memtable.name + "-segment";

        // Convert the memtable to an SSTable
        ISegmentGenerator generator = new MemtableSegmentGenerator();
        generator.fromMemtable(segmentName, memtable);

        // Update the active memtables
        List<Memtable> fullMemtables = this.activeMemtables.getFullMemtablesReadWriteMode();
        List<Memtable> updatedFullMemtables = new ArrayList<>(fullMemtables.size() - 1);

        for (Memtable fullMemtable : fullMemtables) {
            if (fullMemtable.name.equals(memtable.name)) {
                continue;
            }

            updatedFullMemtables.add(fullMemtable);
        }

        this.activeMemtables.updateFullMemtables(updatedFullMemtables);

        // Update the active SSTables info
        List<String> activeSSTables = this.activeSSTables.getActiveSSTables();
        List<String> updatedActiveSSTables = new ArrayList<>(activeSSTables.size() + 1);
        updatedActiveSSTables.add(segmentName);

        for (String activeSSTable : activeSSTables) {
            updatedActiveSSTables.add(activeSSTable);
        }

        this.activeSSTables.updateActiveSSTables(updatedActiveSSTables);
        logger.info("Memtable to SSTable persistence thread finished, memtable: " + memtable.name + " was successfully converted to an SSTable");
    }
}
