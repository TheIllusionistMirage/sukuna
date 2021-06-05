package app.sukuna.sukunaengine.service.worker;

import app.sukuna.sukunaengine.service.operation.WriteOperation;

public interface IWriterWorker {
    WriteOperation retrievePendingWriteOperation() throws InterruptedException;
    void processWriteOperation(WriteOperation writeOperation);
}
