package app.sukuna.sukunaengine.service;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.Configuration;
import app.sukuna.sukunaengine.core.memtable.Memtable;
import app.sukuna.sukunaengine.service.command.IClientCommandParser;
import app.sukuna.sukunaengine.service.command.TcpClientCommandParser;
import app.sukuna.sukunaengine.service.operation.OperationQueue;
import app.sukuna.sukunaengine.service.operation.ReadOperation;
import app.sukuna.sukunaengine.service.operation.WriteOperation;
import app.sukuna.sukunaengine.service.thread.IncomingRequestHandler;
import app.sukuna.sukunaengine.service.worker.ReaderWorker;
import app.sukuna.sukunaengine.service.worker.WriterWorker;

public class SukunaService {
    // private Memtable currentMemtable;
    // private List<Memtable> fullMemtables;
    private AtomicBoolean running;
    protected final ActiveMemtables activeMemtables;
    protected final ActiveSSTables activeSSTables;
    private List<ReaderWorker> readerWorkers;
    private WriterWorker writerWorker;
    // TODO: Add the "X" thread
    public OperationQueue operationQueue;
    private ServerSocket serverSocket;
    private IClientCommandParser clientCommandParser;
    private static final Logger logger = LoggerFactory.getLogger(SukunaService.class);

    // Call default constructor when database engine starting with no previous data
    public SukunaService() {
        this.running = new AtomicBoolean(false);

        // Initialize the shared operation queue
        this.operationQueue = new OperationQueue();

        // Initialize the first memtable
        this.activeMemtables = new ActiveMemtables();

        // Initialize the active SSTables (initially empty)
        this.activeSSTables = new ActiveSSTables();

        // Initialize the reader workers
        this.readerWorkers = new ArrayList<>();
        this.readerWorkers.add(new ReaderWorker("ReaderWorker-1", this.activeMemtables, this.activeSSTables, this.operationQueue));
        this.readerWorkers.add(new ReaderWorker("ReaderWorker-2", this.activeMemtables, this.activeSSTables, this.operationQueue));
        this.readerWorkers.add(new ReaderWorker("ReaderWorker-3", this.activeMemtables, this.activeSSTables, this.operationQueue));

        // Initialize the writer workers
        this.writerWorker = new WriterWorker("WriterWorker-1", this.activeMemtables, this.operationQueue);

        // Initialize the client command parser
        this.clientCommandParser = new TcpClientCommandParser();
    }

    // TODO: Call special parameterized constructor/initialize function when database engine restarting or starting with previously available data

    public void start() {
        logger.info("Preparing to start Sukuna DB engine service...");
        logger.info("Binding service to port: " + Configuration.SukunaServicePort);
        this.running.set(true);

        // Start the reader/writer worker threads
        logger.info("Initializing reader and writer workers...");
        for (ReaderWorker readerWorker : this.readerWorkers) {
            // readerWorker.run();
            readerWorker.start();
        }
        // this.writerWorker.run();
        this.writerWorker.start();

        // Create the server socket
        try {
            this.serverSocket = new ServerSocket(Configuration.SukunaServicePort);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            logger.error("SukunaService: Error occurred when created server socket on port " + Configuration.SukunaServicePort);
            e.printStackTrace();
        }

        logger.info("Started Sukuna DB engine service [OK]");
        // while (this.running.get()) {
        while (true) {
            Socket clientSocket;

            try {
                clientSocket = this.serverSocket.accept();
                logger.trace("New client connected to register operation");
                new IncomingRequestHandler(clientSocket, this.clientCommandParser, this.operationQueue).start();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                logger.error("SukunaService: Error occurred when attempting to accept incoming client request");
                e.printStackTrace();
            }
        }
    }

    public void stop() {
        this.running.set(false);

        // First flush the operation queue
        // TODO: Find a better way to flush read/write queues
        this.operationQueue.pendingReadOperations.clear();
        this.operationQueue.pendingWriteOperations.clear();

        // Stop the reader and writer workers
        for (ReaderWorker readerWorker : this.readerWorkers) {
            readerWorker.stopWorker();
        }
        this.writerWorker.stopWorker();
    }

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
