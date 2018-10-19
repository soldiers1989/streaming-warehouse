package com.tree.finance.bigdata.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class ShutDownSocketListener {

    private Service service;

    private String shutdownKey;

    private boolean stop = false;

    private Logger LOG = LoggerFactory.getLogger(ShutDownSocketListener.class);

    private Thread stopThread;

    private ServerSocket serverSocket;

    private int port;

    public ShutDownSocketListener(Service service, int port) {
        this.service = service;
        this.port = port;
        this.shutdownKey = "ShutdownKey-" + service.getClass().getSimpleName();
    }

    public void init() throws Exception {
        this.serverSocket = new ServerSocket(port);
        this.stopThread = new Thread(this::listen, "SocketShutDownListener");
        stopThread.setDaemon(true);
        this.stopThread.start();
        LOG.info("started ShutDown Socket Listener");
    }

    public void listen() {
        try {
            while (!stop) {
                try (Socket socket = serverSocket.accept();
                     InputStream inp = socket.getInputStream();
                     InputStreamReader isr = new InputStreamReader(inp);
                     BufferedReader bfr = new BufferedReader(isr);) {
                    String info = null;
                    while ((info = bfr.readLine()) != null) {
                        System.out.println(info);
                        if (info.equals(shutdownKey)) {
                            stop = true;
                            break;
                        }
                    }
                    OutputStream ots = socket.getOutputStream();
                    PrintWriter pw = new PrintWriter(ots);
                    if (stop) {
                        pw.write("receive stop command\n");
                    } else {
                        pw.write("not stop command\n");
                    }
                    pw.flush();
                    pw.close();
                    ots.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            serverSocket.close();
            if (stop) {
                service.stop();
            }
        } catch (Exception e) {
            LOG.error("", e);
        }
    }
}
