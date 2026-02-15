package pdc;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {
    
    private final String workerId;
    private final ExecutorService taskThreadPool;
    private final ExecutorService systemThreads;
    private Socket socket;
    private InputStream input;
    private OutputStream output;
    private volatile boolean running = false;
    private volatile boolean connected = false;
    private final Map<Integer, TaskResult> completedTasks = new ConcurrentHashMap<>();
    
    public Worker() {
        this("WORKER_" + System.nanoTime());
    }
    
    public Worker(String workerId) {
        this.workerId = workerId;
        this.taskThreadPool = Executors.newFixedThreadPool(4);  // Handle 4 concurrent tasks
        this.systemThreads = Executors.newCachedThreadPool();
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            socket = new Socket(masterHost, port);
            input = new BufferedInputStream(socket.getInputStream());
            output = new BufferedOutputStream(socket.getOutputStream());
            connected = true;
            running = true;
            
            // Send REGISTER_WORKER message
            Message registerMsg = new Message(
                "REGISTER_WORKER",
                workerId,
                new byte[0]
            );
            
            output.write(registerMsg.pack());
            output.flush();
            
            // Start message receiver thread
            systemThreads.execute(this::receiveMessages);
            
            // Start heartbeat sender thread
            systemThreads.execute(this::sendHeartbeats);
            
        } catch (IOException e) {
            connected = false;
            e.printStackTrace();
        }
    }

    private void receiveMessages() {
        try {
            byte[] buffer = new byte[1024 * 64];
            
            while (running) {
                int bytesRead = input.read(buffer);
                
                if (bytesRead <= 0) {
                    break;
                }
                
                Message msg = Message.unpack(Arrays.copyOf(buffer, bytesRead));
                
                if (msg != null) {
                    if ("WORKER_ACK".equals(msg.type)) {
                        System.out.println("[" + workerId + "] Registered with master");
                    } else if ("RPC_REQUEST".equals(msg.type)) {
                        // Execute the task asynchronously
                        taskThreadPool.execute(() -> executeTask(msg));
                    } else if ("HEARTBEAT".equals(msg.type)) {
                        // Respond to heartbeat
                        try {
                            Message response = new Message("HEARTBEAT", workerId, new byte[0]);
                            synchronized (output) {
                                output.write(response.pack());
                                output.flush();
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        } catch (IOException e) {
            // Connection lost
            connected = false;
        } finally {
            running = false;
        }
    }

    private void sendHeartbeats() {
        try {
            while (running) {
                Thread.sleep(3000);  // Send heartbeat every 3 seconds
                
                Message heartbeat = new Message("HEARTBEAT", workerId, new byte[0]);
                synchronized (output) {
                    output.write(heartbeat.pack());
                    output.flush();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void executeTask(Message msg) {
        try {
            ByteBuffer payload = ByteBuffer.wrap(msg.payload);
            
            // Parse task parameters
            int startRow = payload.getInt();
            int endRow = payload.getInt();
            int matrixAOffset = payload.getInt();
            int matrixBOffset = payload.getInt();
            
            // Simulate matrix operation (in real implementation, perform actual computation)
            Thread.sleep(500);  // Simulate work
            
            // Send response
            ByteBuffer responsePayload = ByteBuffer.allocate(4);
            responsePayload.putInt(1);  // Task ID (would be extracted from message in real implementation)
            
            Message response = new Message(
                "RPC_RESPONSE",
                workerId,
                responsePayload.array()
            );
            
            synchronized (output) {
                output.write(response.pack());
                output.flush();
            }
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            sendTaskError();
        } catch (IOException e) {
            e.printStackTrace();
            sendTaskError();
        }
    }

    private void sendTaskError() {
        try {
            Message error = new Message(
                "TASK_ERROR",
                workerId,
                ByteBuffer.allocate(4).putInt(0).array()
            );
            
            synchronized (output) {
                output.write(error.pack());
                output.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Executes a received task block.
     */
    public void execute() {
        // Tasks are executed via receiveMessages -> executeTask
        // This method could be used for explicit execution trigger if needed
    }

    public void shutdown() {
        running = false;
        connected = false;
        
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        taskThreadPool.shutdown();
        systemThreads.shutdown();
    }

    public boolean isConnected() {
        return connected;
    }

    public String getWorkerId() {
        return workerId;
    }

    private static class TaskResult {
        int taskId;
        byte[] result;
        boolean success;
        
        TaskResult(int taskId, byte[] result, boolean success) {
            this.taskId = taskId;
            this.result = result;
            this.success = success;
        }
    }
}
