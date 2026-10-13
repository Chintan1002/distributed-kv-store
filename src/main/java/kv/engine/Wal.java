package kv.engine;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Write-ahead log: an append-only file of PUT/DELETE records, fsynced on
 * every append. "Log first, apply second" is the entire durability
 * guarantee.
 *
 * <p>On-disk record format (manual framing, not Java Serializable):
 * <pre>
 *   record := op(1 byte) keyLen(int32) keyBytes [valueLen(int32) valueBytes]
 *   op     := 1 = PUT (has value), 2 = DELETE (no value)
 * </pre>
 * Integers are big-endian (ByteBuffer default); strings are UTF-8.
 *
 * <p>A crash mid-append can leave a truncated final record; {@link #replay}
 * stops cleanly at the first incomplete record, so everything acked before
 * the crash is still recovered.
 */
public final class Wal implements Closeable {

    private static final byte OP_PUT = 1;
    private static final byte OP_DELETE = 2;

    /** Callback invoked for each record recovered during replay. */
    public interface ReplayHandler {
        void onPut(String key, String value);

        void onDelete(String key);
    }

    private final Path file;
    private FileChannel channel;

    /** Opens (creating if absent) the log for appending. */
    public Wal(Path file) throws IOException {
        this.file = file;
        this.channel = FileChannel.open(file,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    /**
     * Empties the log after a MemTable flush: everything in it is now durable
     * in an fsynced SSTable, so replaying it would be redundant (though safe —
     * replay is idempotent, which is what makes a crash between SSTable write
     * and this reset harmless).
     */
    public synchronized void reset() throws IOException {
        channel.close();
        channel = FileChannel.open(file,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    public synchronized void appendPut(String key, String value) throws IOException {
        byte[] k = key.getBytes(StandardCharsets.UTF_8);
        byte[] v = value.getBytes(StandardCharsets.UTF_8);
        ByteBuffer record = ByteBuffer.allocate(1 + 4 + k.length + 4 + v.length);
        record.put(OP_PUT).putInt(k.length).put(k).putInt(v.length).put(v);
        writeAndSync(record.flip());
    }

    public synchronized void appendDelete(String key) throws IOException {
        byte[] k = key.getBytes(StandardCharsets.UTF_8);
        ByteBuffer record = ByteBuffer.allocate(1 + 4 + k.length);
        record.put(OP_DELETE).putInt(k.length).put(k);
        writeAndSync(record.flip());
    }

    private void writeAndSync(ByteBuffer record) throws IOException {
        while (record.hasRemaining()) {
            channel.write(record);
        }
        // Real fsync before the write can be acked; flush() alone only
        // reaches the OS page cache, not the physical disk.
        channel.force(true);
    }

    /**
     * Reads every complete record from the log, oldest first. A truncated
     * record at the tail (torn write from a crash) ends replay silently;
     * anything before it was durable and is delivered.
     */
    public static void replay(Path file, ReplayHandler handler) throws IOException {
        if (!Files.exists(file)) {
            return;
        }
        ByteBuffer buf = ByteBuffer.wrap(Files.readAllBytes(file));
        while (buf.hasRemaining()) {
            buf.mark();
            byte op = buf.get();
            String key = readString(buf);
            if (key == null) {
                return; // torn tail
            }
            if (op == OP_PUT) {
                String value = readString(buf);
                if (value == null) {
                    return; // torn tail
                }
                handler.onPut(key, value);
            } else if (op == OP_DELETE) {
                handler.onDelete(key);
            } else {
                return; // unknown op: corrupt tail, stop
            }
        }
    }

    /** Reads a length-prefixed UTF-8 string, or null if the buffer is truncated. */
    private static String readString(ByteBuffer buf) {
        if (buf.remaining() < 4) {
            return null;
        }
        int len = buf.getInt();
        if (len < 0 || buf.remaining() < len) {
            return null;
        }
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
