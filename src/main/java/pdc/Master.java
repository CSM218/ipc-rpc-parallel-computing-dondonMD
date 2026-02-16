import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 * Handles Stragglers (slow workers) and Partitions (disconnected workers).
 */
public class Master {

    private static final String MAGIC = Message.MAGIC;
    private static final int HEARTBEAT_TIMEOUT_MS = 30_000;
    private static final int TASK_TIMEOUT_MS = 60_000;

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private volatile ServerSocket serverSocket;
    private final Map<String, WorkerHandle> workers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> taskResults = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> taskAssignedTo = new ConcurrentHashMap<>();
    private final BlockingQueue<TaskSpec> pendingReassign = new LinkedBlockingQueue<>();
    private volatile Map<String, TaskSpec> currentTaskSpecs = new ConcurrentHashMap<>();
    private volatile boolean listening;

    private static class WorkerHandle {
        final String workerId;
        final Socket socket;
        final DataInputStream dataIn;
        final DataOutputStream dataOut;
        final AtomicLong lastHeartbeat = new AtomicLong(System.currentTimeMillis());

        WorkerHandle(String workerId, Socket socket, DataInputStream dataIn, DataOutputStream dataOut) {
            this.workerId = workerId;
            this.socket = socket;
            this.dataIn = dataIn;
            this.dataOut = dataOut;
        }
    }

    private static class TaskSpec {
        final String taskId;
        final String operation;
        final String payload;

        TaskSpec(String taskId, String operation, String payload) {
            this.taskId = taskId;
            this.operation = operation;
            this.payload = payload;
        }
    }

    /**
     * Entry point for distributed computation.
     * Partitions the problem, schedules across workers, aggregates results.
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        List<String> workerIds = new ArrayList<>(workers.keySet());
        if (workerIds.isEmpty()) {
            return null;
        }

        List<TaskSpec> tasks = partitionMatrix(operation, data, workerCount);
        if (tasks.isEmpty()) {
            return aggregateResult(operation, data, Collections.emptyList());
        }

        taskResults.clear();
        taskAssignedTo.clear();
        currentTaskSpecs.clear();
        for (TaskSpec t : tasks) currentTaskSpecs.put(t.taskId, t);
        List<String> taskIds = new ArrayList<>();
        for (TaskSpec t : tasks) {
            taskIds.add(t.taskId);
        }

        int workerIndex = 0;
        for (TaskSpec t : tasks) {
            String wid = workerIds.get(workerIndex % workerIds.size());
            workerIndex++;
            sendTaskToWorker(wid, t);
        }

        long deadline = System.currentTimeMillis() + TASK_TIMEOUT_MS;
        int retryCount = 0;
        while (System.currentTimeMillis() < deadline) {
            if (taskResults.size() >= taskIds.size()) break;
            reconcileState();
            TaskSpec reassign = pendingReassign.poll();
            if (reassign != null) {
                List<String> liveWorkers = new ArrayList<>(workers.keySet());
                if (liveWorkers.isEmpty()) {
                    pendingReassign.add(reassign);
                    continue;
                }
                retryCount++;
                String wid = liveWorkers.get(workerIndex % liveWorkers.size());
                workerIndex++;
                sendTaskToWorker(wid, reassign);
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        List<Object> results = new ArrayList<>();
        for (String taskId : taskIds) {
            Object r = taskResults.get(taskId);
            if (r != null) results.add(r);
        }
        currentTaskSpecs.clear();
        return aggregateResult(operation, data, results);
    }

    private List<TaskSpec> partitionMatrix(String operation, int[][] data, int workerCount) {
        List<TaskSpec> tasks = new ArrayList<>();
        int rows = data.length;
        if (rows == 0) return tasks;
        int cols = data[0].length;
        int chunk = Math.max(1, (rows + workerCount - 1) / workerCount);
        for (int i = 0; i < rows; i += chunk) {
            int end = Math.min(i + chunk, rows);
            StringBuilder payload = new StringBuilder();
            payload.append(i).append(",").append(end).append(",").append(cols);
            for (int r = i; r < end; r++) {
                for (int c = 0; c < cols; c++) {
                    payload.append(",").append(data[r][c]);
                }
            }
            String taskId = "task-" + i + "-" + end;
            tasks.add(new TaskSpec(taskId, operation, payload.toString()));
        }
        return tasks;
    }

    private void sendTaskToWorker(String workerId, TaskSpec t) {
        WorkerHandle h = workers.get(workerId);
        if (h == null) {
            pendingReassign.add(t);
            return;
        }
        taskAssignedTo.put(t.taskId, workerId);
        try {
            Message req = new Message();
            req.messageType = "RPC_REQUEST";
            req.studentId = System.getenv("STUDENT_ID");
            if (req.studentId == null) req.studentId = "master";
            req.timestamp = System.currentTimeMillis();
            req.setPayloadUtf8(t.taskId + ";" + t.operation + ";" + t.payload);
            h.dataOut.write(req.pack());
            h.dataOut.flush();
        } catch (Exception e) {
            taskAssignedTo.remove(t.taskId);
            pendingReassign.add(t);
        }
    }

    private Object aggregateResult(String operation, int[][] data, List<Object> results) {
        if ("SUM".equals(operation)) {
            long sum = 0;
            for (Object r : results) {
                if (r instanceof Long) sum += (Long) r;
                else if (r != null) sum += Long.parseLong(r.toString());
            }
            return sum;
        }
        if (results.size() == 1 && results.get(0) != null) {
            return results.get(0);
        }
        return results.isEmpty() ? null : results;
    }

    /**
     * Start the communication listener using the custom Message protocol.
     */
    public void listen(int port) throws IOException {
        int p = port;
        if (p == 0) {
            String envPort = System.getenv("MASTER_PORT");
            if (envPort != null) p = Integer.parseInt(envPort);
        }
        if (p == 0) p = 0;
        serverSocket = new ServerSocket(p);
        listening = true;
        systemThreads.submit(this::acceptLoop);
    }

    private void acceptLoop() {
        try {
            while (listening && serverSocket != null && !serverSocket.isClosed()) {
                Socket client = serverSocket.accept();
                systemThreads.submit(() -> handleConnection(client));
            }
        } catch (SocketException e) {
            if (listening) {
                // closed
            }
        } catch (IOException e) {
            if (listening) {
                e.printStackTrace();
            }
        }
    }

    private void handleConnection(Socket socket) {
        String workerId = null;
        try {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            Message first = Message.readFromStream(in);
            if (first == null) {
                socket.close();
                return;
            }
            validate(first);
            if ("REGISTER_WORKER".equals(first.messageType) || "CONNECT".equals(first.messageType)) {
                workerId = first.getPayloadUtf8();
                if (workerId == null || workerId.isEmpty()) workerId = "worker-" + socket.getRemoteSocketAddress();
                WorkerHandle h = new WorkerHandle(workerId, socket, in, out);
                workers.put(workerId, h);
                h.lastHeartbeat.set(System.currentTimeMillis());

                Message ack = new Message();
                ack.messageType = "WORKER_ACK";
                ack.studentId = System.getenv("STUDENT_ID");
                if (ack.studentId == null) ack.studentId = "master";
                ack.timestamp = System.currentTimeMillis();
                ack.setPayloadUtf8(workerId);
                out.write(ack.pack());
                out.flush();

                final String id = workerId;
                systemThreads.submit(() -> readLoop(id, in, socket));
                return;
            }
            socket.close();
        } catch (Exception e) {
            if (workerId != null) workers.remove(workerId);
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private void readLoop(String workerId, DataInputStream in, Socket socket) {
        try {
            while (listening && !socket.isClosed()) {
                Message msg = Message.readFromStream(in);
                if (msg == null) break;
                parse(msg);
                WorkerHandle h = workers.get(workerId);
                if (h != null) h.lastHeartbeat.set(System.currentTimeMillis());
                if ("HEARTBEAT".equals(msg.messageType)) {
                    // already updated lastHeartbeat
                } else if ("TASK_COMPLETE".equals(msg.messageType)) {
                    String pl = msg.getPayloadUtf8();
                    int idx = pl.indexOf(';');
                    if (idx > 0) {
                        String taskId = pl.substring(0, idx);
                        String result = pl.substring(idx + 1);
                        try {
                            taskResults.put(taskId, Long.parseLong(result.trim()));
                        } catch (NumberFormatException e) {
                            taskResults.put(taskId, result);
                        }
                        taskAssignedTo.remove(taskId);
                    }
                }
            }
        } catch (Exception e) {
            // connection closed or error
        } finally {
            workers.remove(workerId);
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private void validate(Message msg) {
        if (msg == null) throw new IllegalArgumentException("null message");
        if (!MAGIC.equals(msg.magic)) throw new IllegalArgumentException("invalid magic");
    }

    private void parse(Message msg) {
        validate(msg);
    }

    /**
     * System health check: detect dead workers and re-queue their tasks for reassignment.
     * Retry: reassign failed tasks to remaining workers for fault tolerance.
     */
    public void reconcileState() {
        long now = System.currentTimeMillis();
        List<String> dead = new ArrayList<>();
        for (Map.Entry<String, WorkerHandle> e : workers.entrySet()) {
            if (now - e.getValue().lastHeartbeat.get() > HEARTBEAT_TIMEOUT_MS) {
                dead.add(e.getKey());
            }
        }
        for (String workerId : dead) {
            WorkerHandle h = workers.remove(workerId);
            if (h != null) {
                try { h.socket.close(); } catch (IOException ignored) {}
                for (Map.Entry<String, String> t : new ArrayList<>(taskAssignedTo.entrySet())) {
                    if (workerId.equals(t.getValue())) {
                        TaskSpec spec = currentTaskSpecs.get(t.getKey());
                        if (spec != null) pendingReassign.add(spec);
                        taskAssignedTo.remove(t.getKey());
                    }
                }
            }
        }
    }
}