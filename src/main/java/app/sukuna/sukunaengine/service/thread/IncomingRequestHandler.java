package app.sukuna.sukunaengine.service.thread;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.service.command.IClientCommandParser;
import app.sukuna.sukunaengine.service.command.TcpClientCommand;
import app.sukuna.sukunaengine.service.operation.OperationBase;
import app.sukuna.sukunaengine.service.operation.OperationQueue;
import app.sukuna.sukunaengine.service.operation.ReadOperation;
import app.sukuna.sukunaengine.service.operation.WriteOperation;

public class IncomingRequestHandler extends Thread {
    private Socket clientSocket;
    private IClientCommandParser clientCommandParser;
    private OperationQueue operationQueue;
    private static final Logger logger = LoggerFactory.getLogger(IncomingRequestHandler.class);

    public IncomingRequestHandler(Socket clientSocket, IClientCommandParser clientCommandParser, OperationQueue operationQueue) {
        this.clientSocket = clientSocket;
        this.clientCommandParser = clientCommandParser;
        this.operationQueue = operationQueue;

        logger.trace("Started new " + IncomingRequestHandler.class.getName() + " thread to handle request from client: " + clientSocket.getLocalSocketAddress().toString());
    }

    @Override
    public void run() {
        InputStream clientInputStream = null;
        BufferedReader bufferedReader = null;
        DataOutputStream clientOutputStream = null;
        
        try {
            clientInputStream = clientSocket.getInputStream();
            bufferedReader = new BufferedReader(new InputStreamReader(clientInputStream));
            clientOutputStream = new DataOutputStream(clientSocket.getOutputStream());
        } catch (IOException e) {
            logger.error("An error occurred when attempting to establish input and output streams for client");
            e.printStackTrace();
            return;
        }

        try {
            clientOutputStream.writeBytes("Enter a command: ");
            String clientInput = bufferedReader.readLine();
            logger.trace("Client request command: " + clientInput);

            // Create operation object and add it to appropriate queue
            TcpClientCommand clientCommand = new TcpClientCommand(clientInput);
            OperationBase operation = this.clientCommandParser.parseClientCommand(clientSocket, clientCommand);

            if (operation == null) {
                logger.trace("The connection to the client was closed");
            }
            if (operation instanceof ReadOperation) {
                //this.operationQueue.pendingReadOperations.put((ReadOperation) operation);
                this.operationQueue.enqueueReadOperation((ReadOperation) operation);
            }
            else if (operation instanceof WriteOperation) {
                // this.operationQueue.pendingWriteOperations.put((WriteOperation) operation);
                this.operationQueue.enqueueWriteOperation((WriteOperation) operation);
            }
        } catch (Exception e) {
            logger.error("An error occurred when attempting to read client request command for client");
            e.printStackTrace();
            try {
                clientSocket.close();
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            return;
        }
    }
}
