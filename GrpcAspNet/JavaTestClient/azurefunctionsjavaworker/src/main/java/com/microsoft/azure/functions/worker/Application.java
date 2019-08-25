package com.microsoft.azure.functions.worker;


import sun.tools.jar.Main;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * The entry point of the Java Language Worker. Every component could get the command line options from this singleton
 * Application instance, and typically that instance will be passed to your components as constructor arguments.
 */
public final class Application {

    public static void main(String[] args) {
        String[] args_array =  args[0].split(":");

        String workerId =  args[1];
        String requestId = "request_1";
        try {
            FunctionRpcGrpcServer server = new FunctionRpcGrpcServer(49150, workerId);
            server.start();
        } catch(Exception ex) {
            System.out.println(ex);
            System.exit(-1);
        }

        try (JavaWorkerClient client = new JavaWorkerClient(args_array)) {

            final InputStream inputStream = Main.class.getResourceAsStream("/logging.properties");
            try
            {
                System.out.println("hello122");
                System.out.println(inputStream.read());
                LogManager.getLogManager().readConfiguration(inputStream);
            }
            catch (final IOException e)
            {
                Logger.getAnonymousLogger().severe("Could not load default logging.properties file");
                Logger.getAnonymousLogger().severe(e.getMessage());
            }

            System.out.println("hello1");
            client.listen(workerId, requestId).get();
        } catch (Exception ex) {
            System.out.println(ex);
            System.exit(-1);
        }
    }

    public static String version() {
        String jarVersion = Application.class.getPackage().getImplementationVersion();
        return jarVersion != null && !jarVersion.isEmpty() ? jarVersion : "Unknown";
    }
}
