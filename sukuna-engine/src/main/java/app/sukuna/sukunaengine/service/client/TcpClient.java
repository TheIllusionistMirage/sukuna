package app.sukuna.sukunaengine.service.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.exceptions.ClientClosureException;
import app.sukuna.sukunaengine.core.exceptions.ClientInputStreamException;
import app.sukuna.sukunaengine.core.exceptions.ClientOutputStreamException;
import app.sukuna.sukunaengine.utils.ErrorHandlingUtils;

public class TcpClient extends ClientBase {
    public final Socket clientSocket;
    public static final Logger logger = LoggerFactory.getLogger(TcpClient.class);

    public TcpClient(String name, Socket clientSocket) {
        super(name);
        this.clientSocket = clientSocket;
    }

    @Override
    public void closure() throws ClientClosureException {
        try {
            this.clientSocket.close();
        } catch (IOException ioException) {
            String errorMsg = "Error occurred when attempting to close socket: "
                + this.clientSocket.getInetAddress().getHostAddress();
            logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(errorMsg, ioException));
            throw new ClientClosureException(errorMsg);
        }
    }

    @Override
    public OutputStream getOutputStream() throws ClientOutputStreamException {
        try {
            return this.clientSocket.getOutputStream();
        } catch (IOException ioException) {
            String errorMsg = "Error occurred when attempting to retrieve the output stream for socket: "
                + this.clientSocket.getInetAddress().getHostAddress();
            logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(errorMsg, ioException));
            throw new ClientOutputStreamException(errorMsg);
        }
    }

    @Override
    public InputStream getInputStream() throws ClientInputStreamException {
        try {
            return this.clientSocket.getInputStream();
        } catch (IOException ioException) {
            String errorMsg = "Error occurred when attempting to retrieve the input stream for socket: "
                + this.clientSocket.getInetAddress().getHostAddress();
            logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(errorMsg, ioException));
            throw new ClientInputStreamException(errorMsg);
        }
    }
}
