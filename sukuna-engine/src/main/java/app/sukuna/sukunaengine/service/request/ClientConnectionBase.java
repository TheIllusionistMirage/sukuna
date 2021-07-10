package app.sukuna.sukunaengine.service.request;

import app.sukuna.sukunaengine.service.client.ClientBase;
import app.sukuna.sukunaengine.service.command.ClientCommandBase;
import app.sukuna.sukunaengine.service.command.IClientCommandParser;

public abstract class ClientConnectionBase {
    // public final IClientCommandParser clientCommandParser;

    // public ClientRequestBase(IClientCommandParser clientCommandParser) {
    //     this.clientCommandParser = clientCommandParser;
    // }

    public abstract ClientBase getClient();
    public abstract ClientCommandBase getCommand();
}
