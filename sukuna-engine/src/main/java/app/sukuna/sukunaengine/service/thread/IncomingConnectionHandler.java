package app.sukuna.sukunaengine.service.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.exceptions.InvalidClientCommandException;
import app.sukuna.sukunaengine.service.command.ClientCommandBase;
import app.sukuna.sukunaengine.service.command.IClientCommandParser;
import app.sukuna.sukunaengine.service.operation.OperationBase;
import app.sukuna.sukunaengine.service.operation.OperationQueue;
import app.sukuna.sukunaengine.service.operation.ReadOperation;
import app.sukuna.sukunaengine.service.operation.WriteOperation;
import app.sukuna.sukunaengine.service.request.ClientConnectionBase;

public class IncomingConnectionHandler extends Thread {
    private ClientConnectionBase clientConnection;
    private IClientCommandParser clientCommandParser;
    private OperationQueue operationQueue;
    private static final Logger logger = LoggerFactory.getLogger(IncomingConnectionHandler.class);

    public IncomingConnectionHandler(ClientConnectionBase clientConnection, IClientCommandParser clientCommandParser, OperationQueue operationQueue) {
        this.setName("IncomingRequestHandler");
        this.clientConnection = clientConnection;
        this.clientCommandParser = clientCommandParser;
        this.operationQueue = operationQueue;

        logger.trace("Started new " + IncomingConnectionHandler.class.getName() + " thread to handle request from new client");
    }

    @Override
    public void run() {
        ClientCommandBase clientCommand = clientConnection.getCommand();
        OperationBase operation;
        try {
            operation = this.clientCommandParser.parseClientCommand(clientConnection.getClient(), clientCommand);
        } catch (InvalidClientCommandException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        }

        if (operation == null) {
            logger.trace("The connection to the client was closed");
        }
        if (operation instanceof ReadOperation) {
            this.operationQueue.enqueueReadOperation((ReadOperation) operation);
        }
        else if (operation instanceof WriteOperation) {
            this.operationQueue.enqueueWriteOperation((WriteOperation) operation);
        }
    }
}
