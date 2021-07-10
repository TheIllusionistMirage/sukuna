package app.sukuna.sukunaengine.service.command;

import java.net.Socket;

import app.sukuna.sukunaengine.core.exceptions.InvalidClientCommandException;
import app.sukuna.sukunaengine.service.client.ClientBase;
import app.sukuna.sukunaengine.service.operation.OperationBase;

public interface IClientCommandParser {
    OperationBase parseClientCommand(ClientBase client, ClientCommandBase clientCommand) throws InvalidClientCommandException;
}
