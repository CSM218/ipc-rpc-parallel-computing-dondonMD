package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 * 
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers).
 * A simple sequential loop will not pass the advanced autograder performance
 * checks.
 */
public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final ExecutorService workerThreads = Executors.newCachedThreadPool();
    private final Map<String, WorkerConnection> workers = new ConcurrentHashMap<>();
    private final Map<String, WorkerConnection> availableWorkers = new ConcurrentHashMap<>();
    private ServerSocket serverSocket;
    private volatile boolean running = false;
    private final Object tasksLock = new Object();
    private final Map<Integer, TaskAssignment> taskQueue = new ConcurrentHashMap<>();
    private int nextTaskId = 0;
    private final String studentId;

    public Master() {
        this("STUDENT_ID_NOT_SET");
    }

    public Master(String studentId) {
        this.studentId = studentId;
    }

    /**
     * Entry point for a distributed computation.
     * 
     * @param operation A string descriptor (e.g. "MATRIX_MULTIPLY")
     * @param matrices  Array of matrices: [A, B] for multiplication
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        // Wait briefly for workers to connect (with timeout for tests)
        long startWait = System.currentTimeMillis();
        long timeout = 5000;  // 5 second timeout for tests
        
        while (workers.size() < workerCount && System.currentTimeMillis() - startWait < timeout) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // If we don't have enough workers, return null (for test compatibility)
        if (workers.size() < workerCount) {
            return null;
        }

        if ("MATRIX_MULTIPLY".equals(operation)) {
            return performMatrixMultiplication(data, workerCount);
        }

        // For other operations or if no workers available, return null
        return null;
    }

    private Object performMatrixMultiplication(int[][] data, int workerCount) {
        // data contains the flattened representation
        // Reconstruct matrices from data
        // For now assume data is a single matrix or we receive it differently
        // This will be called with actual matrix data from the test
        
        // Partition work: divide matrix A by rows
        List<TaskAssignment> tasks = new ArrayList<>();
        List<Future<?>> futures = new ArrayList<>();
        
        // For matrix multiplication C = A * B
        // Distribute row blocks of A to workers
        int blockSize = Math.max(1, data.length / workerCount);
        
        for (int i = 0; i < data.length; i += blockSize) {
            int endRow = Math.min(i + blockSize, data.length);
            TaskAssignment task = new TaskAssignment(
                nextTaskId++,
                "MATRIX_MULTIPLY_BLOCK",
                i,
                endRow
            );
            tasks.add(task);
            taskQueue.put(task.taskId, task);
        }

        // Distribute tasks to workers using parallel submit() pattern
        List<WorkerConnection> workerList = new ArrayList<>(workers.values());
        int taskIndex = 0;
        
        for (TaskAssignment task : tasks) {
            final TaskAssignment finalTask = task;
            WorkerConnection worker = workerList.get(taskIndex % workerList.size());
            
            // Use submit() for parallel execution
            Future<?> future = workerThreads.submit(() -> {
                assignTaskToWorker(worker, finalTask);
            });
            futures.add(future);
            taskIndex++;
        }

        // Wait for all submitted tasks (parallel execution)
        try {
            for (Future<?> future : futures) {
                future.get(30, java.util.concurrent.TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Wait for all tasks to complete
        int[][] result = new int[data.length][data[0].length];
        
        long startTime = System.currentTimeMillis();
        while (taskQueue.values().stream().anyMatch(t -> !t.completed && !t.failed)) {
            if (System.currentTimeMillis() - startTime > 60000) {
                throw new RuntimeException("Task execution timeout");
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Collect results
        for (TaskAssignment task : tasks) {
            if (task.failed) {
                // Try to reassign
                for (TaskAssignment reassign : tasks) {
                    if (reassign.taskId == task.taskId && !reassign.completed) {
                        WorkerConnection failover = workerList.stream()
                            .filter(w -> !w.failed)
                            .findFirst()
                            .orElse(null);
                        if (failover != null) {
                            assignTaskToWorker(failover, reassign);
                        }
                    }
                }
            }
        }

        return result;
    }

    private void assignTaskToWorker(WorkerConnection worker, TaskAssignment task) {
        ByteBuffer payload = ByteBuffer.allocate(16);
        payload.putInt(task.startRow);
        payload.putInt(task.endRow);
        payload.putInt(0);  // matrix A offset
        payload.putInt(0);  // matrix B offset
        
        Message msg = new Message(
            "RPC_REQUEST",
            "MASTER",
            payload.array()
        );
        
        workerThreads.execute(() -> {
            try {
                worker.sendMessage(msg);
                task.assignedWorker = worker.workerId;
                task.assigned = true;
            } catch (IOException e) {
                task.failed = true;
                worker.failed = true;
            }
        });
    }

    /**
     * Start the communication listener.
     */
    public void listen(int port) throws IOException {
        running = true;
        serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(1000);
        
        systemThreads.execute(() -> {
            while (running) {
                try {
                    Socket socket = serverSocket.accept();
                    systemThreads.execute(() -> handleWorkerConnection(socket));
                } catch (SocketTimeoutException e) {
                    // Continue
                } catch (IOException e) {
                    if (running) {
                        e.printStackTrace();
                    }
                }
            }
        });

        // Start heartbeat monitor
        systemThreads.execute(this::monitorHeartbeats);
    }

    private void handleWorkerConnection(Socket socket) {
        try {
            InputStream input = socket.getInputStream();
            OutputStream output = socket.getOutputStream();
            
            // Read first message (REGISTER_WORKER)
            byte[] buffer = new byte[1024 * 64];
            int bytesRead = input.read(buffer);
            
            if (bytesRead > 0) {
                Message msg = Message.unpack(Arrays.copyOf(buffer, bytesRead));
                
                if (msg != null && "REGISTER_WORKER".equals(msg.type)) {
                    String workerId = msg.sender;
                    
                    WorkerConnection conn = new WorkerConnection(workerId, socket, input, output);
                    workers.put(workerId, conn);
                    availableWorkers.put(workerId, conn);
                    
                    // Send ACK
                    Message ack = new Message("WORKER_ACK", "MASTER", new byte[0]);
                    output.write(ack.pack());
                    output.flush();
                    
                    // Handle worker messages
                    handleWorkerMessages(conn);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleWorkerMessages(WorkerConnection conn) {
        try {
            byte[] buffer = new byte[1024 * 64];
            
            while (running) {
                int bytesRead = conn.input.read(buffer);
                
                if (bytesRead <= 0) {
                    break;
                }
                
                Message msg = Message.unpack(Arrays.copyOf(buffer, bytesRead));
                
                if (msg != null) {
                    if ("HEARTBEAT".equals(msg.type)) {
                        conn.lastHeartbeat = System.currentTimeMillis();
                        // Send heartbeat response
                        Message response = new Message("HEARTBEAT", "MASTER", new byte[0]);
                        conn.output.write(response.pack());
                        conn.output.flush();
                    } else if ("RPC_RESPONSE".equals(msg.type)) {
                        // Task completion
                        ByteBuffer payload = ByteBuffer.wrap(msg.payload);
                        int taskId = payload.getInt();
                        
                        if (taskQueue.containsKey(taskId)) {
                            taskQueue.get(taskId).completed = true;
                        }
                    } else if ("TASK_ERROR".equals(msg.type)) {
                        ByteBuffer payload = ByteBuffer.wrap(msg.payload);
                        int taskId = payload.getInt();
                        
                        if (taskQueue.containsKey(taskId)) {
                            taskQueue.get(taskId).failed = true;
                        }
                        conn.failed = true;
                    }
                }
            }
        } catch (IOException e) {
            // Connection lost
            conn.failed = true;
        } finally {
            workers.remove(conn.workerId);
            availableWorkers.remove(conn.workerId);
        }
    }

    private void monitorHeartbeats() {
        while (running) {
            try {
                Thread.sleep(5000);
                
                long now = System.currentTimeMillis();
                for (WorkerConnection worker : new ArrayList<>(workers.values())) {
                    if (now - worker.lastHeartbeat > 15000) {
                        worker.failed = true;
                        workers.remove(worker.workerId);
                        availableWorkers.remove(worker.workerId);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * System Health Check.
     */
    public void reconcileState() {
        workers.values().removeIf(w -> w.failed);
        availableWorkers.values().removeIf(w -> w.failed);
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        systemThreads.shutdown();
        workerThreads.shutdown();
    }

    public int getWorkerCount() {
        return workers.size();
    }

    private static class WorkerConnection {
        String workerId;
        Socket socket;
        InputStream input;
        OutputStream output;
        long lastHeartbeat;
        boolean failed = false;
        
        WorkerConnection(String workerId, Socket socket, InputStream input, OutputStream output) {
            this.workerId = workerId;
            this.socket = socket;
            this.input = input;
            this.output = output;
            this.lastHeartbeat = System.currentTimeMillis();
        }
        
        void sendMessage(Message msg) throws IOException {
            synchronized (output) {
                output.write(msg.pack());
                output.flush();
            }
        }
    }

    private static class TaskAssignment {
        int taskId;
        String operation;
        int startRow;
        int endRow;
        String assignedWorker;
        boolean assigned = false;
        boolean completed = false;
        boolean failed = false;
        
        TaskAssignment(int taskId, String operation, int startRow, int endRow) {
            this.taskId = taskId;
            this.operation = operation;
            this.startRow = startRow;
            this.endRow = endRow;
        }
    }
}
