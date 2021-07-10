package app.sukuna.sukunaengine.service.operation;

import app.sukuna.sukunaengine.core.exceptions.InvalidClientException;
import app.sukuna.sukunaengine.service.client.ClientBase;

public abstract class OperationBase {
    // TODO: Introduce abstraction for a client (e.g., a client can also be another class instead of a TCP socket)
    // public final Socket client;
    public final ClientBase client;

    public OperationBase(ClientBase client) throws InvalidClientException {
        if (client == null) {
            throw new InvalidClientException("Invalid client used to attempt initialization of a " + this.getClass().getName() + " instance");
        }
        this.client = client;
    }
}
