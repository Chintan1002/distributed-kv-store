package kv.engine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Hand-rolled Bloom filter over a {@code long[]} bit-array: answers "is this
 * key definitely NOT in the SSTable?" in O(k) with zero disk I/O. False
 * positives are possible (we scan the file for nothing), false negatives are
 * NOT — which is what makes it safe as a pure skip-check.
 *
 * <p>Sizing: for n keys and target false-positive rate p,
 * bits m = -n·ln(p)/(ln2)² and hash count k = (m/n)·ln2. The k indexes are
 * derived from one SHA-256 digest via double hashing
 * (h_i = h1 + i·h2, Kirsch–Mitzenmacher), so we hash each key exactly once.
 */
public final class BloomFilter {

    // MessageDigest isn't thread-safe, and
    // a ThreadLocal avoids re-allocating one per lookup on the hot read path.
    private static final ThreadLocal<MessageDigest> SHA256 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    });

    private final long[] words;
    private final int numBits;
    private final int numHashes;

    private BloomFilter(long[] words, int numBits, int numHashes) {
        this.words = words;
        this.numBits = numBits;
        this.numHashes = numHashes;
    }

    public static BloomFilter create(long expectedEntries, double falsePositiveRate) {
        long n = Math.max(1, expectedEntries);
        int numBits = (int) Math.ceil(-n * Math.log(falsePositiveRate) / (Math.log(2) * Math.log(2)));
        numBits = Math.max(64, numBits);
        int numHashes = Math.max(1, (int) Math.round((double) numBits / n * Math.log(2)));
        return new BloomFilter(new long[(numBits + 63) / 64], numBits, numHashes);
    }

    public void add(String key) {
        long[] h = hashPair(key);
        for (int i = 0; i < numHashes; i++) {
            int bit = bitIndex(h, i);
            words[bit >>> 6] |= 1L << (bit & 63);
        }
    }

    /** false = definitely absent (skip the file); true = probably present (go scan). */
    public boolean mightContain(String key) {
        long[] h = hashPair(key);
        for (int i = 0; i < numHashes; i++) {
            int bit = bitIndex(h, i);
            if ((words[bit >>> 6] & 1L << (bit & 63)) == 0) {
                return false;
            }
        }
        return true;
    }

    private static long[] hashPair(String key) {
        MessageDigest digest = SHA256.get();
        digest.reset();
        ByteBuffer d = ByteBuffer.wrap(digest.digest(key.getBytes(StandardCharsets.UTF_8)));
        return new long[] {d.getLong(), d.getLong()};
    }

    private int bitIndex(long[] hashPair, int i) {
        long combined = hashPair[0] + (long) i * hashPair[1];
        return (int) Math.floorMod(combined, numBits);
    }

    // Sidecar persistence: numHashes(int32) numBits(int32) wordCount(int32) words(int64...)

    public void writeTo(Path file) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4 + 4 + 4 + words.length * 8);
        buf.putInt(numHashes).putInt(numBits).putInt(words.length);
        for (long word : words) {
            buf.putLong(word);
        }
        buf.flip();
        try (FileChannel channel = FileChannel.open(file,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
            channel.force(true);
        }
    }

    public static BloomFilter readFrom(Path file) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(Files.readAllBytes(file));
        int numHashes = buf.getInt();
        int numBits = buf.getInt();
        long[] words = new long[buf.getInt()];
        for (int i = 0; i < words.length; i++) {
            words[i] = buf.getLong();
        }
        return new BloomFilter(words, numBits, numHashes);
    }
}
