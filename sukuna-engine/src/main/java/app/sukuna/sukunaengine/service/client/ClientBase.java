package app.sukuna.sukunaengine.service.client;

import java.io.InputStream;
import java.io.OutputStream;

import app.sukuna.sukunaengine.core.exceptions.ClientClosureException;
import app.sukuna.sukunaengine.core.exceptions.ClientInputStreamException;
import app.sukuna.sukunaengine.core.exceptions.ClientOutputStreamException;

public abstract class ClientBase {
    public final String name;

    public ClientBase(String name) {
        this.name = name;
    }

    // Gets executed at the end of any read/write operation on a client
    public abstract void closure() throws ClientClosureException;

    public abstract OutputStream getOutputStream() throws ClientOutputStreamException;

    public abstract InputStream getInputStream() throws ClientInputStreamException;
}
