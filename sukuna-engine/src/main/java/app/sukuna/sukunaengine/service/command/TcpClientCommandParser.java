package app.sukuna.sukunaengine.service.command;

import java.io.IOException;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.exceptions.InvalidClientCommandException;
import app.sukuna.sukunaengine.core.exceptions.InvalidClientException;
import app.sukuna.sukunaengine.service.client.ClientBase;
import app.sukuna.sukunaengine.service.client.TcpClient;
import app.sukuna.sukunaengine.service.operation.OperationBase;
import app.sukuna.sukunaengine.service.operation.ReadOperation;
import app.sukuna.sukunaengine.service.operation.WriteOperation;

public class TcpClientCommandParser implements IClientCommandParser {
    private final static Logger logger = LoggerFactory.getLogger(TcpClientCommandParser.class);

    @Override
    public OperationBase parseClientCommand(ClientBase client, ClientCommandBase clientCommand) throws InvalidClientCommandException {
        TcpClient tcpClient = (TcpClient) client;
        TcpClientCommand tcpClientCommand = (TcpClientCommand) clientCommand;

        if (tcpClient == null) {
            String errorMsg = "Error occurred while parsing TCP Client command, invalid client object passed to parser";
            logger.error(errorMsg);
            throw new InvalidClientCommandException(errorMsg);
        }

        if (tcpClientCommand == null) {
            String errorMsg = "Error occurred while parsing TCP Client command, invalid client command object passed to parser";
            logger.error(errorMsg);
            throw new InvalidClientCommandException(errorMsg);
        }

        String[] commandTokens = tcpClientCommand.command.split(" ");

        // First argument is the command type, i.e., "set" or "get" or "quit" etc.
        String commandType = commandTokens[0];

        if (commandType.equals("quit")) {
            logger.trace("Client decided to quit after connecting, closing connection");
            try {
                tcpClient.clientSocket.close();
                return null;
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
        if (commandTokens.length < 2) {
            // error
            String errorMsg = "Too few arguments provided in command: " + tcpClientCommand.command;
            logger.error(errorMsg);
            throw new InvalidClientCommandException(errorMsg);
        }

        // Possibly the command: set <key> <value>
        if (commandType.equals("set") && commandTokens.length == 3) {
            String key = commandTokens[1];
            String value = commandTokens[2];

            try {
                //return (OperationBase)(new WriteOperation(clientSocket, key, value));
                return new WriteOperation(tcpClient, key, value);
            } catch (InvalidClientException e) {
                // TODO Auto-generated catch block
                logger.error("Unable to create WriteOperation object");
                e.printStackTrace();
                return null;
            }
        }
        // Possible the command: get <key>
        else if (commandType.equals("get") && commandTokens.length == 2) {
            String key = commandTokens[1];

            try {
                //return (OperationBase)(new WriteOperation(clientSocket, key, value));
                return new ReadOperation(tcpClient, key);
            } catch (InvalidClientException e) {
                // TODO Auto-generated catch block
                logger.error("Unable to create WriteOperation object");
                e.printStackTrace();
                return null;
            }
        }

        // If control reached here then something is wrong
        String errorMsg = "Invalid command specified: " + tcpClientCommand.command + ", returning back null operation object";
        logger.error(errorMsg);
        throw new InvalidClientCommandException(errorMsg);
    }
}