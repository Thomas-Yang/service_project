package edu.umich.clarity.service.util;

import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

/**
 * Helper class for launching the Thrift services.
 *
 * @author Hailong on 6/24/15.
 */
public class TServers {
    public static final int workerCount = 16;

    public static void launchThreadedThriftServer(int port,
                                                  TProcessor processor) throws IOException {
        TServerTransport serverTransport = null;
        try {
            serverTransport = new TServerSocket(port);
        } catch (TTransportException e) {
            e.printStackTrace();
        }
        TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
        args.processor(processor);
        args.minWorkerThreads(workerCount);
        TServer server = new TThreadPoolServer(args);
        new Thread(new TServerRunnable(server)).start();
    }

    private static class TServerRunnable implements Runnable {
        private TServer server;

        public TServerRunnable(TServer server) {
            this.server = server;
        }

        @Override
        public void run() {
            this.server.serve();
        }

    }
}
