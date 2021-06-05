package app.sukuna.sukunaengine.service.operation;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import app.sukuna.sukunaengine.core.Configuration;

public class OperationQueue {
    private final BlockingQueue<ReadOperation> pendingReadOperations;
    private final BlockingQueue<WriteOperation> pendingWriteOperations;

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
        this.pendingReadOperations.clear();
    }

    public void flushPendingWriteOperations() {
        this.pendingWriteOperations.clear();
    }
}
