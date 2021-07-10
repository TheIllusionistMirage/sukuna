package app.sukuna.sukunaservice;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.exceptions.ApplicationException;
import app.sukuna.sukunaengine.service.ActiveMemtables;
import app.sukuna.sukunaengine.service.ActiveSSTables;
import app.sukuna.sukunaengine.service.ClientConnectionsQueue;
import app.sukuna.sukunaengine.service.SukunaEngineMainThread;
import app.sukuna.sukunaengine.service.client.TcpClient;
import app.sukuna.sukunaengine.service.operation.OperationQueue;
import app.sukuna.sukunaengine.service.request.TcpClientConnection;
import app.sukuna.sukunaengine.utils.ErrorHandlingUtils;

public class SukunaService {
    private static ClientConnectionsQueue clientRequestsQueue;
    private static SukunaEngineMainThread sukunaEngine;
    private static ServerSocket serverSocket;
    private static int port = 6969;
    private static AtomicBoolean running = new AtomicBoolean(false);
    private final static Logger logger = LoggerFactory.getLogger(SukunaService.class);

    public static void main(String[] args) throws ApplicationException, InterruptedException {
        // Handle SIGTERM
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                running.set(false);

                this.setName("ShutdownHook");

                logger.info("Received SIGTERM, stopping Sukuna service...");

                if (sukunaEngine != null && sukunaEngine.isRunning()) {
                    sukunaEngine.stopEngine();
                }

                logger.info("Stopped Sukuna service");

                org.apache.logging.log4j.LogManager.shutdown();
            }
        });

        logger.info("Starting Sukuna Service...");

        // if (args.length != 2) {
        // throw new ApplicationException(String.format("Usage: java -jar
        // sukuna-service.jar <path-to-configuration-file> <port>"));
        // }

        // String pathToConfigurationFile = args[0];
        // port = Integer.parseInt(args[1]);

        // Initialize engine
        clientRequestsQueue = new ClientConnectionsQueue();

        sukunaEngine = new SukunaEngineMainThread(clientRequestsQueue, new ActiveMemtables(), new ActiveSSTables(),
                new OperationQueue());

        // Start engine
        sukunaEngine.start();

        running.set(true);

        // Listen for incoming client requests via TCP
        listen();
    }

    private static void listen() {
        // Create the server socket
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Bound to port: " + port, ", listening for incoming client connections");
        } catch (IOException ioException) {
            logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(
                    "SukunaService: Error occurred when created server socket on port " + port, ioException));
        }

        while (running.get()) {
            Socket clientSocket;

            try {
                // Accept an incoming TCP client connection
                clientSocket = serverSocket.accept();

                // Add the incoming connection to the client connection queue
                TcpClientConnection tcpClientRequest = new TcpClientConnection(
                        new TcpClient(clientSocket.getInetAddress().getHostName(), clientSocket));
                clientRequestsQueue.enqueueIncomingConnection(tcpClientRequest);

                logger.trace("New client connected to register operation");
            } catch (IOException ioException) {
                logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(
                        "SukunaService: Error occurred when attempting to accept incoming client request",
                        ioException));
            }
        }

        logger.info("Stopped listening to incoming connections");
    }
}
