package app.sukuna.sukunaengine.service.worker;

import app.sukuna.sukunaengine.service.operation.ReadOperation;

public interface IReaderWorker {
    ReadOperation retrievePendingReadOperation() throws InterruptedException;
    void processReadOperation(ReadOperation readOperation);
}
