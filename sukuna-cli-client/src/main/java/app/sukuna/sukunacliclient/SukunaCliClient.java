package app.sukuna.sukunacliclient;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SukunaCliClient {
    private final static String quitCommand = "quit";
    private static Socket serverSocket;
    private static boolean running = true;
    private final static Logger logger = LoggerFactory.getLogger(SukunaCliClient.class);

    public static void main(String[] args) {
        // Handle SIGTERM
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                SukunaCliClient.stopClient();
            }
        });

        StringBuilder sb = new StringBuilder();
        
        sb.append("░██████╗██╗░░░██╗██╗░░██╗██╗░░░██╗███╗░░██╗░█████╗░\n");
        sb.append("██╔════╝██║░░░██║██║░██╔╝██║░░░██║████╗░██║██╔══██╗\n");
        sb.append("╚█████╗░██║░░░██║█████═╝░██║░░░██║██╔██╗██║███████║\n");
        sb.append("░╚═══██╗██║░░░██║██╔═██╗░██║░░░██║██║╚████║██╔══██║\n");
        sb.append("██████╔╝╚██████╔╝██║░╚██╗╚██████╔╝██║░╚███║██║░░██║\n");
        sb.append("╚═════╝░░╚═════╝░╚═╝░░╚═╝░╚═════╝░╚═╝░░╚══╝╚═╝░░╚═╝\n");
        sb.append("                          (Sukuna CLI Client v0.0.1)");

        System.out.println(sb.toString());
        Scanner scanner = new Scanner(System.in);

        while (SukunaCliClient.running) {
            System.out.print("Sukuna [" + Configuration.SukunaServiceAddress + "@" + Configuration.SukunaServicePort + "] > ");
            
            String command;
            try {
                command = scanner.nextLine();
            } catch (Exception exception) {
                // An exception occurs when SIGTERM is initiated by Ctrl+C
                System.out.println(""); // append a newline to the prompt
                continue;
            }

            // If command is an invalid string, simply skip it
            if (command == null || command.isEmpty() || command.trim().isEmpty()) {
                continue;
            }

            // Check if quit command was specified
            if (command.toLowerCase().equals(SukunaCliClient.quitCommand)) {
                SukunaCliClient.stopClient();
                break;
            }

            // Connect to server, send command and handle response
            SukunaCliClient.connectToServer();
            String serverOutput = SukunaCliClient.sendCommandToServer(command);
            System.out.println(serverOutput);
        }

        scanner.close();
        LogManager.shutdown();
    }

    private static void stopClient() {
        SukunaCliClient.running = false;
        SukunaCliClient.connectToServer();
        SukunaCliClient.sendCommandToServer(SukunaCliClient.quitCommand);
    }

    private static void connectToServer() {
        try {
            serverSocket = new Socket(Configuration.SukunaServiceAddress, Configuration.SukunaServicePort);
        } catch (UnknownHostException exception) {
            logger.error("Unable to connect to host: " + Configuration.SukunaServiceAddress + ", port: " + Configuration.SukunaServicePort + ", invalid host/port");
            running = false;
        } catch (IOException exception) {
            logger.error("Unable to connect to host: " + Configuration.SukunaServiceAddress + ", port: " + Configuration.SukunaServicePort + ", no response received from server");
            running = false;
        }
    }

    private static String sendCommandToServer(String command) {
        if (serverSocket == null) {
            logger.warn("Attempt to send a command(" + command + " to the server when not connected to server");
            return null;
        }
        
        DataOutputStream serverOutputStream = null;
        InputStream serverInputStream = null;
        BufferedReader bufferedReader = null;
        try {
            serverOutputStream = new DataOutputStream(serverSocket.getOutputStream());
            serverInputStream = serverSocket.getInputStream();
            bufferedReader = new BufferedReader(new InputStreamReader(serverInputStream));
        } catch (IOException e) {
            logger.error("Unable to initialize " + DataOutputStream.class.getName() + " instance to connect to server\'s output stream");
            return null;
        }

        PrintWriter writer = new PrintWriter(serverOutputStream, true);
        writer.println(command);

        String serverOutput;
        try {
            serverOutput = bufferedReader.readLine();
            return serverOutput;
        } catch (IOException exception) {
            logger.error("Error occurred while trying to read output from server");
            return null;
        }
    }
}
