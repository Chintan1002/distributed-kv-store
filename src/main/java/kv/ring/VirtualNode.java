package kv.ring;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * One of the many ring positions a physical node claims. Scattering each
 * node across {@code index = 0..V-1} positions evens out load (law of large
 * numbers) and spreads a departing node's range over many survivors instead
 * of dumping it on one neighbor.
 */
public record VirtualNode(String nodeId, int index) {

    /** This virtual node's position on the 64-bit ring. */
    public long position() {
        return hash(nodeId + "#" + index);
    }

    /** SHA-256 folded to the first 8 bytes. */
    static long hash(String s) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return ByteBuffer.wrap(digest.digest(s.getBytes(StandardCharsets.UTF_8))).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
