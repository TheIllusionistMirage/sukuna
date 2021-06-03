package app.sukuna.sukunaengine.service.operation;

import java.net.Socket;

import app.sukuna.sukunaengine.core.exceptions.InvalidClientException;

public abstract class OperationBase {
    public final Socket client;

    public OperationBase(Socket client) throws InvalidClientException {
        if (client == null) {
            throw new InvalidClientException("Invalid client used to attempt initialization of a " + this.getClass().getName() + " instance");
        }
        this.client = client;
    }
}
