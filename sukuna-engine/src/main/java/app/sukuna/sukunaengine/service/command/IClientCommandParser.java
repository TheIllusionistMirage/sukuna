package app.sukuna.sukunaengine.service.command;

import java.net.Socket;

import app.sukuna.sukunaengine.core.exceptions.InvalidClientCommandException;
import app.sukuna.sukunaengine.service.operation.OperationBase;

public interface IClientCommandParser {
    // TODO: Once OperationBase gets rid of the direct dependence on Socket, the same abstraction to be used here as well
    OperationBase parseClientCommand(Socket client, ClientCommandBase clientCommand) throws InvalidClientCommandException;
}
