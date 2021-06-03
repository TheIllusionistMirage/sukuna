package app.sukuna.sukunaengine.service;

import java.util.ArrayList;
import java.util.List;

import app.sukuna.sukunaengine.core.memtable.Memtable;
import app.sukuna.sukunaengine.service.operation.OperationBase;
import app.sukuna.sukunaengine.service.operation.OperationQueue;
import app.sukuna.sukunaengine.service.operation.ReadOperation;
import app.sukuna.sukunaengine.service.operation.WriteOperation;
import app.sukuna.sukunaengine.service.worker.ReaderWorker;
import app.sukuna.sukunaengine.service.worker.WriterWorker;

public class SukunaService {
    // private Memtable currentMemtable;
    // private List<Memtable> fullMemtables;
    protected final ActiveMemtables activeMemtables;
    protected final ActiveSSTables activeSSTables;
    private List<ReaderWorker> readerWorkers;
    private WriterWorker writerWorker;
    public final OperationQueue operationQueue;

    // Call default constructor when database engine starting with no previous data
    public SukunaService() {
        // Initialize the shared operation queue
        this.operationQueue = new OperationQueue();

        // Initialize the first memtable
        this.activeMemtables = new ActiveMemtables();

        // Initialize the active SSTables (initially empty)
        this.activeSSTables = new ActiveSSTables();

        // Initialize the reader workers
        this.readerWorkers = new ArrayList<>();
        this.readerWorkers.add(new ReaderWorker(this.activeMemtables, this.activeSSTables, this.operationQueue));
        this.readerWorkers.add(new ReaderWorker(this.activeMemtables, this.activeSSTables, this.operationQueue));
        this.readerWorkers.add(new ReaderWorker(this.activeMemtables, this.activeSSTables, this.operationQueue));

        // Initialize the writer workers
        this.writerWorker = new WriterWorker(this.activeMemtables, this.operationQueue);
    }

    // TODO: Call special parameterized constructor/initialize function when database engine restarting or starting with previously available data

    public void updateActiveMemtables(Memtable currentMemtable, List<Memtable> fullMemtables) {
        this.activeMemtables.updateCurrentMemtable(currentMemtable);
        this.activeMemtables.updateFullMemtables(fullMemtables);
    }

    public void updateActiveSSTables(List<String> activeSSTables) {
        this.activeSSTables.updateActiveSSTables(activeSSTables);
    }

    public void enqueueReadOperation(ReadOperation readOperation) {
        try {
            this.operationQueue.pendingReadOperations.put(readOperation);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void enqueueWriteOperation(WriteOperation writeOperation) {
        try {
            this.operationQueue.pendingWriteOperations.put(writeOperation);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
