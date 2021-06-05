package app.sukuna.sukunaengine.core.exceptions;

public class InvalidClientCommandException extends Exception {
    public InvalidClientCommandException(String errorMsg) {
        super(errorMsg);
    }
}
