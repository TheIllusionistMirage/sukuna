package app.sukuna.sukunaengine.service.operation;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.configuration.Configuration;

public class OperationQueue {
    private final BlockingQueue<ReadOperation> pendingReadOperations;
    private final BlockingQueue<WriteOperation> pendingWriteOperations;
    private final static Logger logger = LoggerFactory.getLogger(OperationQueue.class);

    public OperationQueue() {
        this.pendingReadOperations = new LinkedBlockingQueue<>(Configuration.MaxPendingReadOperationQueueSize);
        this.pendingWriteOperations = new LinkedBlockingQueue<>(Configuration.MaxPendingWriteOperationQueueSize);
    }

    public void enqueueReadOperation(ReadOperation readOperation) {
        try {
            this.pendingReadOperations.put(readOperation);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void enqueueWriteOperation(WriteOperation writeOperation) {
        try {
            this.pendingWriteOperations.put(writeOperation);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public ReadOperation retrievePendingReadOperation() throws InterruptedException {
        return this.pendingReadOperations.take();
    }

    public WriteOperation retrievePendingWriteOperation() throws InterruptedException {
        return this.pendingWriteOperations.take();
    }

    public void flushPendingReadOperations() {
        logger.info("Flushing pending read operations...");
        int pendingOperationsCount = this.pendingReadOperations.size();
        this.pendingReadOperations.clear();
        logger.info(String.format("Flushed %d pending read operations [OK]", pendingOperationsCount));
    }

    public void flushPendingWriteOperations() {
        logger.info("Flushing pending write operations...");
        int pendingOperationsCount = this.pendingWriteOperations.size();
        this.pendingWriteOperations.clear();
        logger.info(String.format("Flushed %d pending write operations [OK]", pendingOperationsCount));
    }
}
