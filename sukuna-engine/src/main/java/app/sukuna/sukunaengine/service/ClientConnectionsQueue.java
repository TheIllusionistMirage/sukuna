package app.sukuna.sukunaengine.service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.configuration.Configuration;
import app.sukuna.sukunaengine.service.request.ClientConnectionBase;
import app.sukuna.sukunaengine.utils.ErrorHandlingUtils;

public class ClientConnectionsQueue {
    private final BlockingQueue<ClientConnectionBase> connections;
    private final static Logger logger = LoggerFactory.getLogger(ClientConnectionsQueue.class);

    public ClientConnectionsQueue() {
        this.connections = new LinkedBlockingQueue<>(Configuration.MaxPendingIncomingRequestQueueSize);
    }

    public void enqueueIncomingConnection(ClientConnectionBase connection) {
        try {
            this.connections.put(connection);
        } catch (InterruptedException exception) {
            logger.error("Error(s) occurred while enqueing incoming client connection: " + ErrorHandlingUtils.getFormattedExceptionDetails(exception));
        }
    }

    public ClientConnectionBase retrieveConnection() throws InterruptedException {
        return this.connections.take();
    }

    public void flushUnhandledConnections() {
        logger.info("Flushing client connections pending to be processed...");
        int pendingRequestsCount = this.connections.size();
        this.connections.clear();
        logger.info(String.format("Flushed %d client connections pending to be processed [OK]", pendingRequestsCount));
    }
}
