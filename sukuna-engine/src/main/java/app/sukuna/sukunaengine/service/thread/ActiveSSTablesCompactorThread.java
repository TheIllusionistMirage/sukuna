package app.sukuna.sukunaengine.service.thread;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.compactor.ICompactor;
import app.sukuna.sukunaengine.compactor.SSTableCompactor;
import app.sukuna.sukunaengine.core.index.ImmutableInMemoryIndex;
import app.sukuna.sukunaengine.core.segment.SSTable;
import app.sukuna.sukunaengine.core.segment.SegmentBase;
import app.sukuna.sukunaengine.service.ActiveSSTables;

public class ActiveSSTablesCompactorThread extends Thread {
    private ActiveSSTables activeSSTables;
    private final static Logger logger = LoggerFactory.getLogger(ActiveSSTablesCompactorThread.class);

    public ActiveSSTablesCompactorThread(ActiveSSTables activeSSTables) {
        this.activeSSTables = activeSSTables;
    }

    // TODO: Handle interrupt
    @Override
    public void run() {
        logger.info("Active SSTable compaction initiated.");
        // Initial list of SSTables before compaction
        List<String> initialSSTableNameList = this.activeSSTables.getActiveSSTables();
        
        String initialSSTableList = "";
        for (String sstableName : initialSSTableNameList) {
            initialSSTableList += sstableName + " ";
        }

        logger.info("Active SSTable name list before compaction: " + initialSSTableList);

        // Construct SSTable objects based on the segment files
        SegmentBase[] segments = new SegmentBase[initialSSTableNameList.size()];
        
        // TODO: Loop optimization
        for (int i = 0; i < initialSSTableNameList.size(); ++i) {
            String segmentName = initialSSTableNameList.get(i);
            
            ImmutableInMemoryIndex index = new ImmutableInMemoryIndex(segmentName);
            index.initialize(segmentName);

            SSTable sstable = new SSTable();
            sstable.initialize(segmentName, index);

            segments[i] = sstable;
        }
        
        ICompactor compactor = new SSTableCompactor();
        SegmentBase[] compactedSegments = compactor.compact(segments);

        List<String> activeSSTableNameList = this.activeSSTables.getActiveSSTables();
        List<String> updatedActiveSSTableNameList = new ArrayList<>();

        activeSSTableNameList.removeAll(initialSSTableNameList);

        String currentActiveSSTableNameList = "";
        for (String activeSSTableName : activeSSTableNameList) {
            updatedActiveSSTableNameList.add(activeSSTableName);
            currentActiveSSTableNameList += activeSSTableName + " ";
        }

        // TODO: Also add the list of the newly compacted sstables to this list before updating
        for (SegmentBase segment : compactedSegments) {
            updatedActiveSSTableNameList.add(segment.name);
            currentActiveSSTableNameList += segment.name + " ";
        }

        this.activeSSTables.updateActiveSSTables(updatedActiveSSTableNameList);

        logger.info("Active SSTable compaction completed, current active SSTable name list: " + currentActiveSSTableNameList);
    }
}
