package app.sukuna.sukunaengine.service.operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.exceptions.InvalidClientException;
import app.sukuna.sukunaengine.service.client.ClientBase;

public class ReadOperation extends OperationBase {
    public final String key;
    public static final Logger logger = LoggerFactory.getLogger(ReadOperation.class);

    public ReadOperation(ClientBase client, String key) throws InvalidClientException {
        super(client);

        this.key = key;
        logger.trace("Initialized read operation object to read key: " + key + " by client: " + this.client.name);
    }
}
