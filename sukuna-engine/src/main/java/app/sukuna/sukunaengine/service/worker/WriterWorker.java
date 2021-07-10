package app.sukuna.sukunaengine.service.worker;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.exceptions.ClientClosureException;
import app.sukuna.sukunaengine.core.exceptions.ClientOutputStreamException;
import app.sukuna.sukunaengine.service.ActiveMemtables;
import app.sukuna.sukunaengine.service.operation.OperationQueue;
import app.sukuna.sukunaengine.service.operation.WriteOperation;
import app.sukuna.sukunaengine.utils.ErrorHandlingUtils;

public class WriterWorker extends Thread implements IWriterWorker {
    private final String name;
    private AtomicBoolean running;
    private final ActiveMemtables activeMemtables;
    private final OperationQueue operationQueue;
    private static final Logger logger = LoggerFactory.getLogger(WriterWorker.class);

    public WriterWorker(String name, ActiveMemtables activeMemtables, OperationQueue operationQueue) {
        this.name = name;
        this.setName(this.name);
        this.running = new AtomicBoolean(false);
        this.activeMemtables = activeMemtables;
        this.operationQueue = operationQueue;
    }

    // Thread overrides
    @Override
    public void run() {
        this.running.set(true);

        logger.info("Started writer worker [OK]");
        try {
            while (this.running.get()) {
                WriteOperation writeOperation = this.retrievePendingWriteOperation();
                this.processWriteOperation(writeOperation);
            }
        } catch (InterruptedException interruptedException) {
            if (this.running.get()) {
                logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(
                        "Fetching a pending write operation from the write queue was interrupted",
                        interruptedException));
            }
        }
    }

    // IWriterWorker overrides
    @Override
    public WriteOperation retrievePendingWriteOperation() throws InterruptedException {
        // return this.operationQueue.pendingWriteOperations.take();
        return this.operationQueue.retrievePendingWriteOperation();
    }

    @Override
    public void processWriteOperation(WriteOperation writeOperation) {
        logger.trace("Write operation being handled by writer worker: " + this.name);
        // TODO: Perform actual write operation
        // Already can determine the current memtable using
        // this.activeMemtables.getCurrentMemtableReadWriteMode()

        String key = writeOperation.key;
        String value = writeOperation.value;

        // First check if the key exists in the current memtable
        this.activeMemtables.getCurrentMemtableReadWriteMode().upsert(key, value);

        // TODO: Use writeOperation.client to send value back
        this.sendResponseToClient(writeOperation);
    }

    public synchronized void stopWorker() {
        this.running.set(false);
        logger.info(String.format("Stopped writer worker thread %s", this.name));
        this.interrupt();
    }

    private void sendResponseToClient(WriteOperation writeOperation) {
        try {
            DataOutputStream clientOutputStream = new DataOutputStream(writeOperation.client.getOutputStream());
            clientOutputStream.writeBytes("Successfully added record with key: " + writeOperation.key + " and value: "
                    + writeOperation.value);
            logger.trace(
                    "Successfully wrote value (" + writeOperation.value + ") for key (" + writeOperation.key + ")");
        } catch (ClientOutputStreamException clientWriteException) {
            logger.error(ErrorHandlingUtils.getFormattedExceptionDetails("Unable to get client output stream",
                    clientWriteException));
        } catch (IOException ioException) {
            logger.error(ErrorHandlingUtils.getFormattedExceptionDetails("Unable to write to client output stream",
                    ioException));
        } finally {
            try {
                writeOperation.client.closure();
            } catch (ClientClosureException clientClosureException) {
                logger.error(ErrorHandlingUtils.getFormattedExceptionDetails("Unable to perform closure on client",
                        clientClosureException));
            }
        }
    }
}
