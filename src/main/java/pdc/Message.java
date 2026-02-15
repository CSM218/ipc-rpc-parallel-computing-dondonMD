package pdc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Custom Wire Format:
 * [Magic (6 bytes)] [Version (4 bytes)] [MessageType Length (2 bytes)] [MessageType (variable)]
 * [StudentId Length (2 bytes)] [StudentId (variable)] [Timestamp (8 bytes)]
 * [Payload Length (4 bytes)] [Payload (variable)]
 * 
 * Total frame length is prefixed with 4 bytes at the start for streaming protocols.
 */
public class Message {
    public String magic;
    public int version;
    public String messageType;
    public String studentId;
    public long timestamp;
    public byte[] payload;
    
    // Convenience fields for backward compatibility
    public String type;
    public String sender;

    public Message() {
        this.magic = "CSM218";
        this.version = 1;
        this.timestamp = System.currentTimeMillis();
        this.payload = new byte[0];
    }

    public Message(String messageType, String studentId, byte[] payload) {
        this();
        this.messageType = messageType;
        this.type = messageType;  // For backward compatibility
        this.studentId = studentId;
        this.sender = studentId;  // For backward compatibility
        this.payload = payload != null ? payload : new byte[0];
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Format: [Total Length (4)] [Magic (6)] [Version (4)] [MessageType] [StudentId] [Timestamp (8)] [Payload]
     */
    public byte[] pack() {
        byte[] magicBytes = magic.getBytes(StandardCharsets.UTF_8);
        byte[] messageTypeBytes = (messageType != null ? messageType : type).getBytes(StandardCharsets.UTF_8);
        byte[] studentIdBytes = (studentId != null ? studentId : sender).getBytes(StandardCharsets.UTF_8);
        
        // Calculate the body size (everything except the 4-byte length prefix)
        int bodySize = magicBytes.length + 4 + 2 + messageTypeBytes.length + 2 + 
                       studentIdBytes.length + 8 + 4 + payload.length;
        
        ByteBuffer buffer = ByteBuffer.allocate(4 + bodySize);
        
        // Write total frame length (excluding this 4-byte field itself)
        buffer.putInt(bodySize);
        
        // Write magic
        buffer.put(magicBytes);
        
        // Write version
        buffer.putInt(version);
        
        // Write messageType with length prefix
        buffer.putShort((short) messageTypeBytes.length);
        buffer.put(messageTypeBytes);
        
        // Write studentId with length prefix
        buffer.putShort((short) studentIdBytes.length);
        buffer.put(studentIdBytes);
        
        // Write timestamp
        buffer.putLong(timestamp);
        
        // Write payload with length prefix
        buffer.putInt(payload.length);
        buffer.put(payload);
        
        return buffer.array();
    }

    /**
     * Reconstructs a Message from a byte stream.
     * Assumes the buffer contains [Total Length (4)] followed by the message body.
     */
    public static Message unpack(byte[] data) {
        if (data.length < 4) {
            return null;
        }
        
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        try {
            int frameLength = buffer.getInt();
            
            // Validate frame length
            if (data.length < frameLength + 4) {
                return null;
            }
            
            // Read magic
            byte[] magicBytes = new byte[6];
            buffer.get(magicBytes);
            String magic = new String(magicBytes, StandardCharsets.UTF_8);
            
            // Read version
            int version = buffer.getInt();
            
            // Read messageType
            short messageTypeLength = buffer.getShort();
            byte[] messageTypeBytes = new byte[messageTypeLength];
            buffer.get(messageTypeBytes);
            String messageType = new String(messageTypeBytes, StandardCharsets.UTF_8);
            
            // Read studentId
            short studentIdLength = buffer.getShort();
            byte[] studentIdBytes = new byte[studentIdLength];
            buffer.get(studentIdBytes);
            String studentId = new String(studentIdBytes, StandardCharsets.UTF_8);
            
            // Read timestamp
            long timestamp = buffer.getLong();
            
            // Read payload
            int payloadLength = buffer.getInt();
            byte[] payload = new byte[payloadLength];
            buffer.get(payload);
            
            Message message = new Message();
            message.magic = magic;
            message.version = version;
            message.messageType = messageType;
            message.type = messageType;  // For backward compatibility
            message.studentId = studentId;
            message.sender = studentId;  // For backward compatibility
            message.timestamp = timestamp;
            message.payload = payload;
            
            return message;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Extracts a complete message frame from a stream buffer.
     * Returns the byte array of one complete message, or null if incomplete.
     */
    public static byte[] extractFrame(ByteBuffer streamBuffer) {
        if (streamBuffer.remaining() < 4) {
            return null;
        }
        
        // Peek at frame length without consuming it
        int frameLength = streamBuffer.getInt(streamBuffer.position());
        
        if (streamBuffer.remaining() < frameLength + 4) {
            return null;
        }
        
        // Extract the complete frame
        byte[] frame = new byte[frameLength + 4];
        streamBuffer.get(frame);
        return frame;
    }

    /**
     * Serialize the message to a wire format for transmission.
     * This is an alias for pack() to match test expectations.
     */
    public byte[] serialize() {
        return pack();
    }

    /**
     * Deserialize a message from wire format.
     * This is an alias for unpack() to match test expectations.
     */
    public static Message deserialize(byte[] data) {
        return unpack(data);
    }
}
