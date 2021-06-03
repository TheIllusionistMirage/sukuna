package app.sukuna.sukunaengine.service.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.service.ActiveMemtables;
import app.sukuna.sukunaengine.service.operation.OperationQueue;
import app.sukuna.sukunaengine.service.operation.WriteOperation;

public class WriterWorker implements IWriterWorker, Runnable {
    private final ActiveMemtables activeMemtables;
    private final OperationQueue operationQueue;
    private static final Logger logger = LoggerFactory.getLogger(WriterWorker.class);

    public WriterWorker(ActiveMemtables activeMemtables, OperationQueue operationQueue) {
        this.activeMemtables = activeMemtables;
        this.operationQueue = operationQueue;
    }

    // IWriterWorker overrides
    @Override
    public WriteOperation retrievePendingWriteOperation() throws InterruptedException {
        return this.operationQueue.pendingWriteOperations.take();
    }

    @Override
    public void processWriteOperation(WriteOperation writeOperation) {
        // TODO: Perform actual write operation
        // Already can determine the current memtable using this.activeMemtables.getCurrentMemtableReadWriteMode()

        String key = writeOperation.key;
        String value = writeOperation.value;
        
        // First check if the key exists in the current memtable
        this.activeMemtables.getCurrentMemtableReadWriteMode().upsert(key, value);

        // TODO: Use writeOperation.client to send value back
        this.sendResponseToClient(writeOperation);
    }

    // Runnable overrides
    @Override
    public void run() {
        try {
            while (true) {
                WriteOperation writeOperation = this.retrievePendingWriteOperation();
                this.processWriteOperation(writeOperation);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendResponseToClient(WriteOperation writeOperation) {
        // TODO: Use writeOperation.client to send value back

        // Dummy placeholder for now
        logger.info("Successfully wrote value (" + writeOperation.value + ") for key (" + writeOperation.key + ")");
    }
}
