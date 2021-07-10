package app.sukuna.sukunaengine.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.memtable.Memtable;
import app.sukuna.sukunaengine.service.command.IClientCommandParser;
import app.sukuna.sukunaengine.service.command.TcpClientCommandParser;
import app.sukuna.sukunaengine.service.operation.OperationQueue;
import app.sukuna.sukunaengine.service.request.ClientConnectionBase;
import app.sukuna.sukunaengine.service.thread.IncomingRequestHandler;
import app.sukuna.sukunaengine.service.thread.SukunaServiceManagerThread;
import app.sukuna.sukunaengine.service.worker.ReaderWorker;
import app.sukuna.sukunaengine.service.worker.WriterWorker;

public class SukunaEngineMainThread extends Thread {
    private volatile AtomicBoolean running;
    protected final ActiveMemtables activeMemtables;
    protected final ActiveSSTables activeSSTables;
    private List<ReaderWorker> readerWorkers;
    private WriterWorker writerWorker;
    public OperationQueue operationQueue;
    public ClientConnectionsQueue clientRequestsQueue;
    private IClientCommandParser clientCommandParser;
    private SukunaServiceManagerThread serviceManager;
    private static final Logger logger = LoggerFactory.getLogger(SukunaEngineMainThread.class);

    // Call default constructor when database engine starting with no previous data
    public SukunaEngineMainThread(ClientConnectionsQueue clientRequestsQueue, ActiveMemtables activeMemtables, ActiveSSTables activeSSTables, OperationQueue operationQueue) {
        this.setName("SukunaEngineMainThread");
        this.running = new AtomicBoolean(false);

        this.clientRequestsQueue = clientRequestsQueue;

        // Initialize the first memtable
        this.activeMemtables = activeMemtables;

        // Initialize the active SSTables (initially empty)
        this.activeSSTables = activeSSTables;

        // Initialize the shared operation queue
        this.operationQueue = operationQueue;

        // Initialize the reader workers
        this.readerWorkers = new ArrayList<>();
        this.readerWorkers.add(new ReaderWorker("ReaderWorker-1", this.activeMemtables, this.activeSSTables, this.operationQueue));
        this.readerWorkers.add(new ReaderWorker("ReaderWorker-2", this.activeMemtables, this.activeSSTables, this.operationQueue));
        this.readerWorkers.add(new ReaderWorker("ReaderWorker-3", this.activeMemtables, this.activeSSTables, this.operationQueue));

        // Initialize the writer workers
        this.writerWorker = new WriterWorker("WriterWorker-1", this.activeMemtables, this.operationQueue);

        // Initialize the client command parser
        this.clientCommandParser = new TcpClientCommandParser();

        // Initialize the service manager thread
        this.serviceManager = new SukunaServiceManagerThread(this.activeMemtables, this.activeSSTables);
    }

    // TODO: Call special parameterized constructor/initialize function when database engine restarting or starting with previously available data

    @Override
    public void run() {
        this.startEngine();
    }

    public void startEngine() {
        logger.info("Preparing to start Sukuna DB engine service...");
        this.running.set(true);

        // Start the reader/writer worker threads
        logger.info("Initializing reader and writer workers...");
        for (ReaderWorker readerWorker : this.readerWorkers) {
            readerWorker.start();
        }
        this.writerWorker.start();

        // Start the service manager thread
        this.serviceManager.start();
        logger.info("Started the SukunaServiceManager thread for service management [OK]");

        // Wait until all threads are running
        while (true) {
            boolean readerWorkersAlive = true;
            for (ReaderWorker readerWorker : this.readerWorkers) {
                readerWorkersAlive = readerWorkersAlive && readerWorker.isAlive();
            }

            if (readerWorkersAlive && this.writerWorker.isAlive() && this.serviceManager.isAlive()) {
                break;
            }
        }

        logger.info("Started Sukuna DB engine service [OK]");
        while (this.running.get()) {
            ClientConnectionBase request = this.retrieveIncomingClientRequest();

            if (request == null) {
                continue;
            }

            logger.trace("New client connected to register operation");
            
            // TODO: This is not practical on a large scale, use a threadpool instead
            // Create new thread to handle request
            new IncomingRequestHandler(request, this.clientCommandParser, this.operationQueue).start();
        }
    }

    public boolean isRunning() {
        return this.running.get();
    }

    public void stopEngine() {
        logger.info("Stopping Sukuna Engine...");
        
        this.running.set(false);
        
        this.clientRequestsQueue.flushUnhandledConnections();

        // Stop the reader and writer workers
        logger.info("Stopping reader worker threads...");
        for (ReaderWorker readerWorker : this.readerWorkers) {
            readerWorker.stopWorker();
        }

        logger.info("Stopping writer worker threads...");
        this.writerWorker.stopWorker();

        // First flush the operation queue
        // TODO: Find a better way to flush read/write queues
        this.operationQueue.flushPendingReadOperations();
        this.operationQueue.flushPendingWriteOperations();
        
        // Stop the service manager thread
        this.serviceManager.stopServiceManager();
        
        while (true) {
            boolean readerWorkersAlive = false;
            for (ReaderWorker readerWorker : this.readerWorkers) {
                readerWorkersAlive = readerWorkersAlive && readerWorker.isAlive();
            }

            if (!readerWorkersAlive && !this.writerWorker.isAlive() && !this.serviceManager.isAlive()) {
                break;
            }
        }

        // TODO: Flush active memtables too

        logger.info("Stopped Sukuna Engine [OK]");
        this.interrupt();
    }

    public void updateActiveMemtables(Memtable currentMemtable, List<Memtable> fullMemtables) {
        this.activeMemtables.updateCurrentMemtable(currentMemtable);
        this.activeMemtables.updateFullMemtables(fullMemtables);
    }

    public void updateActiveSSTables(List<String> activeSSTables) {
        this.activeSSTables.updateActiveSSTables(activeSSTables);
    }

    private ClientConnectionBase retrieveIncomingClientRequest() {
        try {
            return this.clientRequestsQueue.retrieveConnection();
        } catch (InterruptedException e) {
            logger.debug("The client request queue was interrupted while waiting");
            return null;
        }
    }
}
