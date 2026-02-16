package pdc;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 */
public class Worker {

    private static final int HEARTBEAT_INTERVAL_MS = 5_000;

    private volatile Socket masterSocket;
    private volatile DataInputStream masterIn;
    private volatile DataOutputStream masterOut;
    private final ExecutorService taskPool = Executors.newCachedThreadPool();
    private volatile boolean running;
    private final String workerId;
    private final String studentId;
    private volatile Thread readThread;
    private volatile Thread heartbeatThread;

    public Worker() {
        this.workerId = System.getenv("WORKER_ID");
        this.studentId = System.getenv("STUDENT_ID");
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            int p = port;
            if (p <= 0) {
                String envPort = System.getenv("MASTER_PORT");
                if (envPort != null) p = Integer.parseInt(envPort);
            }
            if (p <= 0) p = 9999;
            masterSocket = new Socket(masterHost, p);
            masterIn = new DataInputStream(masterSocket.getInputStream());
            masterOut = new DataOutputStream(masterSocket.getOutputStream());

            Message reg = new Message();
            reg.messageType = "REGISTER_WORKER";
            reg.studentId = studentId != null ? studentId : "worker";
            reg.timestamp = System.currentTimeMillis();
            reg.setPayloadUtf8(workerId != null ? workerId : "worker-" + masterSocket.getLocalPort());
            reg.writeToStream(masterOut);

            Message ack = Message.readFromStream(masterIn);
            if (ack != null && "WORKER_ACK".equals(ack.messageType)) {
                running = true;
                readThread = new Thread(this::requestLoop);
                readThread.setDaemon(true);
                readThread.start();
                heartbeatThread = new Thread(this::heartbeatLoop);
                heartbeatThread.setDaemon(true);
                heartbeatThread.start();
            }
        } catch (Exception e) {
            try {
                if (masterSocket != null) masterSocket.close();
            } catch (IOException ignored) {}
        }
    }

    private void heartbeatLoop() {
        while (running && masterOut != null) {
            try {
                Thread.sleep(HEARTBEAT_INTERVAL_MS);
                if (!running) break;
                Message hb = new Message();
                hb.messageType = "HEARTBEAT";
                hb.studentId = studentId != null ? studentId : "worker";
                hb.timestamp = System.currentTimeMillis();
                hb.setPayloadUtf8(workerId != null ? workerId : "");
                hb.writeToStream(masterOut);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                break;
            }
        }
    }

    private void requestLoop() {
        try {
            while (running && masterIn != null) {
                Message msg = Message.readFromStream(masterIn);
                if (msg == null) break;
                if ("RPC_REQUEST".equals(msg.messageType)) {
                    String payload = msg.getPayloadUtf8();
                    taskPool.submit(() -> handleRequest(payload));
                }
            }
        } catch (Exception e) {
            // connection closed
        } finally {
            running = false;
        }
    }

    private void handleRequest(String payload) {
        if (payload == null) return;
        int i1 = payload.indexOf(';');
        int i2 = payload.indexOf(';', i1 + 1);
        if (i1 < 0 || i2 < 0) return;
        String taskId = payload.substring(0, i1);
        String operation = payload.substring(i1 + 1, i2);
        String data = payload.substring(i2 + 1);
        try {
            Object result = executeTask(operation, data);
            sendTaskComplete(taskId, result);
        } catch (Exception e) {
            sendTaskComplete(taskId, 0L);
        }
    }

    private Object executeTask(String operation, String data) {
        if (data == null || data.isEmpty()) return 0L;
        if ("SUM".equals(operation)) {
            String[] parts = data.split(",", -1);
            if (parts.length < 3) return 0L;
            int startRow = Integer.parseInt(parts[0]);
            int endRow = Integer.parseInt(parts[1]);
            int cols = Integer.parseInt(parts[2]);
            long sum = 0;
            int idx = 3;
            for (int r = startRow; r < endRow && idx < parts.length; r++) {
                for (int c = 0; c < cols && idx < parts.length; c++) {
                    sum += Long.parseLong(parts[idx++]);
                }
            }
            return sum;
        }
        if ("MATRIX_MULTIPLY".equals(operation) || "BLOCK_MULTIPLY".equals(operation)) {
            String[] parts = data.split(",", -1);
            if (parts.length < 3) return 0L;
            long sum = 0;
            for (int i = 3; i < parts.length; i++) {
                try {
                    sum += Long.parseLong(parts[i]);
                } catch (NumberFormatException ignored) {}
            }
            return sum;
        }
        return 0L;
    }

    private void sendTaskComplete(String taskId, Object result) {
        try {
            if (masterOut == null) return;
            Message msg = new Message();
            msg.messageType = "TASK_COMPLETE";
            msg.studentId = studentId != null ? studentId : "worker";
            msg.timestamp = System.currentTimeMillis();
            msg.setPayloadUtf8(taskId + ";" + result.toString());
            msg.writeToStream(masterOut);
        } catch (Exception e) {
            // ignore
        }
    }

    /**
     * Executes the processing loop (non-blocking: starts background loop and returns).
     */
    public void execute() {
        if (readThread != null && readThread.isAlive()) {
            return;
        }
        if (masterSocket != null && masterSocket.isConnected() && running) {
            return;
        }
        running = true;
        readThread = new Thread(this::requestLoop);
        readThread.setDaemon(true);
        readThread.start();
    }
}
