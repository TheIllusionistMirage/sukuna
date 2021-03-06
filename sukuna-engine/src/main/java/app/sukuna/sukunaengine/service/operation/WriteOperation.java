package app.sukuna.sukunaengine.service.operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.exceptions.InvalidClientException;
import app.sukuna.sukunaengine.service.client.ClientBase;

public class WriteOperation extends OperationBase {
    public final String key;
    public final String value;
    public static final Logger logger = LoggerFactory.getLogger(ReadOperation.class);

    public WriteOperation(ClientBase client, String key, String value) throws InvalidClientException {
        super(client);

        this.key = key;
        this.value = value;
    }
}
