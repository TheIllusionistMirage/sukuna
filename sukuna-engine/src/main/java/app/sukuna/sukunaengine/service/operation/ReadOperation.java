package app.sukuna.sukunaengine.service.operation;

import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.exceptions.InvalidClientException;

public class ReadOperation extends OperationBase {
    public final String key;
    public static final Logger logger = LoggerFactory.getLogger(ReadOperation.class);

    public ReadOperation(Socket client, String key) throws InvalidClientException {
        super(client);

        this.key = key;
        logger.trace("Initialized read operation object to read key: " + key + " by client: " + this.client.getRemoteSocketAddress().toString());
    }
}
