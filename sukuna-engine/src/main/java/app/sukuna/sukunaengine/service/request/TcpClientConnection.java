package app.sukuna.sukunaengine.service.request;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.service.client.ClientBase;
import app.sukuna.sukunaengine.service.client.TcpClient;
import app.sukuna.sukunaengine.service.command.ClientCommandBase;
import app.sukuna.sukunaengine.service.command.IClientCommandParser;
import app.sukuna.sukunaengine.service.command.TcpClientCommand;

public class TcpClientConnection extends ClientConnectionBase {
    public final TcpClient tcpClient;
    private final static Logger logger = LoggerFactory.getLogger(TcpClientConnection.class);

    public TcpClientConnection(TcpClient tcpClient) {
        // super(clientCommandParser);
        this.tcpClient = tcpClient;
    }

    @Override
    public ClientCommandBase getCommand() {
        InputStream clientInputStream = null;
        BufferedReader bufferedReader = null;
        // DataOutputStream clientOutputStream = null;
        
        try {
            clientInputStream = this.tcpClient.clientSocket.getInputStream();
            bufferedReader = new BufferedReader(new InputStreamReader(clientInputStream));
            // clientOutputStream = new DataOutputStream(this.tcpClient.clientSocket.getOutputStream());
        } catch (IOException e) {
            logger.error("An error occurred when attempting to establish input and output streams for client");
            e.printStackTrace();
            return null;
        }

        try {
            String clientInput = bufferedReader.readLine();
            logger.trace("Client request command: " + clientInput);

            // Create operation object and add it to appropriate queue
            TcpClientCommand clientCommand = new TcpClientCommand(clientInput);
            return clientCommand;
        } catch (Exception e) {
            logger.error("An error occurred when attempting to read client request command for client");
            e.printStackTrace();
            try {
                this.tcpClient.clientSocket.close();
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            return null;
        }
    }

    @Override
    public ClientBase getClient() {
        return this.tcpClient;
    }
}
