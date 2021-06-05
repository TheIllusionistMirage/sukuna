package app.sukuna.sukunaengine.service.command;

public class TcpClientCommand extends ClientCommandBase {
    public final String command;
    
    public TcpClientCommand(String clientCommand) {
        this.command = clientCommand;
    }
}
