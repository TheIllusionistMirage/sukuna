package app.sukuna.sukunaengine.service.operation;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import app.sukuna.sukunaengine.core.Configuration;

public class OperationQueue {
    public final BlockingQueue<ReadOperation> pendingReadOperations;
    public final BlockingQueue<WriteOperation> pendingWriteOperations;

    public OperationQueue() {
        this.pendingReadOperations = new LinkedBlockingQueue<>(Configuration.PendingReadOperationQueueSize);
        this.pendingWriteOperations = new LinkedBlockingQueue<>(Configuration.PendingWriteOperationQueueSize);
    }
}
