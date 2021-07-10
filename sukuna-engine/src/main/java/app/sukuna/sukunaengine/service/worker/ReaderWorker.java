package app.sukuna.sukunaengine.service.worker;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.exceptions.ClientClosureException;
import app.sukuna.sukunaengine.core.exceptions.ClientOutputStreamException;
import app.sukuna.sukunaengine.core.index.ImmutableInMemoryIndex;
import app.sukuna.sukunaengine.core.memtable.Memtable;
import app.sukuna.sukunaengine.core.segment.SSTable;
import app.sukuna.sukunaengine.service.ActiveMemtables;
import app.sukuna.sukunaengine.service.ActiveSSTables;
import app.sukuna.sukunaengine.service.operation.OperationQueue;
import app.sukuna.sukunaengine.service.operation.ReadOperation;
import app.sukuna.sukunaengine.utils.ErrorHandlingUtils;

public class ReaderWorker extends Thread implements IReaderWorker {
    private final String name;
    private AtomicBoolean running;
    private final ActiveMemtables activeMemtables;
    private final ActiveSSTables activeSSTables;
    private final OperationQueue operationQueue;
    // Use the SSTable abstraction to deal with on-disk SSTables
    private HashMap<String, SSTable> sstables;
    private List<String> sstableSequence;
    private static final Logger logger = LoggerFactory.getLogger(ReaderWorker.class);

    public ReaderWorker(String name, ActiveMemtables activeMemtables, ActiveSSTables activeSSTables,
            OperationQueue operationQueue) {
        this.name = name;
        this.setName(this.name);
        this.running = new AtomicBoolean(false);
        this.activeMemtables = activeMemtables;
        this.activeSSTables = activeSSTables;
        this.operationQueue = operationQueue;
        this.sstableSequence = this.activeSSTables.getActiveSSTables();
    }

    // Thread overrides
    @Override
    public void run() {
        this.running.set(true);

        logger.info("Started reader worker [OK]");
        try {
            while (this.running.get()) {
                // Check and optionally update SSTable objects before handling the next read
                // request.
                // meta-TODO: Optimize updating the SSTable info, checking if SSTables
                // updated before each request is really a bad hit to the performance. Current
                // way is HIGHLY unoptimal and asignificant hit to read operation completion
                if (this.SSTableUpdateRequired()) {
                    this.updateSSTables();
                }

                ReadOperation readOperation = this.retrievePendingReadOperation();
                this.processReadOperation(readOperation);
            }
        } catch (InterruptedException interruptedException) {
            if (this.running.get()) {
                logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(
                        "Fetching a pending read operation from the read queue was interrupted", interruptedException));
            }
        }
    }

    // IReaderWorker overrides
    @Override
    public ReadOperation retrievePendingReadOperation() throws InterruptedException {
        // return this.operationQueue.pendingReadOperations.take();
        return this.operationQueue.retrievePendingReadOperation();
    }

    @Override
    public void processReadOperation(ReadOperation readOperation) {
        logger.trace("Read operation being handled by reader worker: " + this.name);
        // Check and optionally update SSTable objects before handling the next read
        // request
        // meta-TODO: Optimize updating the SSTable info, checking if SSTables updated
        // before each request is really a bad hit to the performance. Current way is
        // HIGHLY unoptimal and asignificant hit to read operation completion
        if (this.SSTableUpdateRequired()) {
            this.updateSSTables();
        }

        // TODO: Perform actual read
        // Already can determine the active memtable using
        // this.activeMemtables.getCurrentMemtableReadWriteMode() and
        // this.activeMemtables.getFullMemtablesReadWriteMode()

        String key = readOperation.key;
        String value = null;

        // First check if the key exists in the current memtable
        value = this.activeMemtables.getCurrentMemtableReadWriteMode().read(key);

        if (value != null) {
            // TODO: Use readOperation.client to send value back
            this.sendResponseToClient(readOperation, value);
            return;
        }

        // Next check all full memtables that are pending SST conversion
        for (Memtable memtable : this.activeMemtables.getFullMemtablesReadWriteMode()) {
            value = memtable.read(key);

            if (value != null) {
                // TODO: Use readOperation.client to send value back
                this.sendResponseToClient(readOperation, value);
                return;
            }
        }

        // Next check all SSTs to see if the key exists here
        for (String segmentName : this.sstableSequence) {
            SSTable sstable = this.sstables.get(segmentName);

            // This check will technically NEVER be false
            if (sstable == null) {
                // TODO: throw
            }

            value = sstable.read(key);

            if (value != null) {
                // TODO: Use readOperation.client to send value back
                this.sendResponseToClient(readOperation, value);
                return;
            }
        }

        if (value == null) {
            logger.trace("The key: \"" + readOperation.key + "\" does not exist, no value found");
        }

        // If control has reached here then it means that the key does not exist at all
        // TODO: Use readOperation.client to send error response (NoSuchKey??)
        this.sendResponseToClient(readOperation, null);
    }

    public synchronized void stopWorker() {
        this.running.set(false);
        logger.info(String.format("Stopped reader worker thread %s", this.name));
        this.interrupt();
    }

    private boolean SSTableUpdateRequired() {
        if (this.sstables == null) {
            return true;
        }
        for (String segmentName : this.activeSSTables.getActiveSSTables()) {
            if (!this.sstables.containsKey(segmentName)) {
                return true;
            }
        }
        return false;
    }

    private void updateSSTables() {
        this.sstables = new HashMap<>();
        this.sstableSequence = new ArrayList<>();

        for (String segmentName : this.activeSSTables.getActiveSSTables()) {
            SSTable sstable = new SSTable();

            ImmutableInMemoryIndex index = new ImmutableInMemoryIndex(segmentName);
            index.initialize(segmentName);

            sstable.initialize(segmentName, index);

            this.sstables.put(segmentName, sstable);
            this.sstableSequence.add(segmentName);
        }
    }

    private void sendResponseToClient(ReadOperation readOperation, String value) {
        if (value == null) {
            value = "NoSuchKeyError: The key: " + readOperation.key + " does not exist in the database";
        }

        try {
            DataOutputStream clientOutputStream;
            clientOutputStream = new DataOutputStream(readOperation.client.getOutputStream());
            clientOutputStream.writeBytes(value);
            logger.trace("Successfully handled read operation for key (" + readOperation.key + ")");
        } catch (ClientOutputStreamException clientWriteException) {
            logger.error(ErrorHandlingUtils.getFormattedExceptionDetails("Unable to get client output stream",
                    clientWriteException));
        } catch (IOException ioException) {
            logger.error(ErrorHandlingUtils
                    .getFormattedExceptionDetails("Unable to write data to client's output stream", ioException));
        } finally {
            try {
                readOperation.client.closure();
            } catch (ClientClosureException clientClosureException) {
                logger.error(ErrorHandlingUtils.getFormattedExceptionDetails("Unable to perform closure on client",
                        clientClosureException));
            }
        }
    }
}
