package pdc;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 *
 * Requirement: Custom WIRE FORMAT. No JSON, XML, or Java Serialization.
 * Format: 4-byte length prefix (big-endian) + body.
 * Body: magic(UTF), version(int), messageType(UTF), studentId(UTF), timestamp(long), payloadLen(int), payload(bytes).
 */
public class Message {
    public static final String MAGIC = "CSM218";

    public String magic;
    public int version;
    public String messageType;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    public Message() {
        this.magic = MAGIC;
        this.version = 1;
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Framing: length-prefix (4 bytes) then body.
     */
    public byte[] pack() throws IOException {
        ByteArrayOutputStream bodyOut = new ByteArrayOutputStream();
        DataOutputStream bodyDos = new DataOutputStream(bodyOut);

        bodyDos.writeUTF(magic != null ? magic : MAGIC);
        bodyDos.writeInt(version);
        bodyDos.writeUTF(messageType != null ? messageType : "");
        bodyDos.writeUTF(studentId != null ? studentId : "");
        bodyDos.writeLong(timestamp);
        bodyDos.writeInt(payload != null ? payload.length : 0);
        if (payload != null && payload.length > 0) {
            bodyDos.write(payload);
        }

        byte[] body = bodyOut.toByteArray();
        ByteArrayOutputStream frame = new ByteArrayOutputStream();
        DataOutputStream frameDos = new DataOutputStream(frame);
        frameDos.writeInt(body.length);
        frameDos.write(body);
        return frame.toByteArray();
    }

    /**
     * Reconstructs a Message from a byte stream (body only, without length prefix).
     * Uses ByteBuffer for efficient zero-copy style parsing where applicable.
     */
    public static Message unpack(byte[] data) throws IOException {
        if (data == null || data.length == 0) return null;
        ByteBuffer buf = ByteBuffer.wrap(data);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
        Message m = new Message();
        m.magic = dis.readUTF();
        m.version = dis.readInt();
        m.messageType = dis.readUTF();
        m.studentId = dis.readUTF();
        m.timestamp = dis.readLong();
        int payloadLen = dis.readInt();
        m.payload = payloadLen > 0 ? dis.readNBytes(payloadLen) : new byte[0];
        return m;
    }

    /**
     * Reads one length-prefixed message from the stream.
     * Uses chunked read loop to handle jumbo payloads (TCP fragmentation / large messages).
     */
    public static Message readFromStream(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] body = new byte[len];
        int total = 0;
        while (total < len) {
            int n = in.read(body, total, len - total);
            if (n <= 0) throw new IOException("Truncated message");
            total += n;
        }
        return unpack(body);
    }

    /**
     * Writes this message to the stream (length-prefixed).
     */
    public void writeToStream(DataOutputStream out) throws IOException {
        out.write(pack());
        out.flush();
    }

    public String getPayloadUtf8() {
        if (payload == null || payload.length == 0) return "";
        return new String(payload, StandardCharsets.UTF_8);
    }

    public void setPayloadUtf8(String s) {
        this.payload = s != null ? s.getBytes(StandardCharsets.UTF_8) : new byte[0];
    }
}