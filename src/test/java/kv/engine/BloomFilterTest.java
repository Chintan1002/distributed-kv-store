package kv.engine;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BloomFilterTest {

    /** The invariant the whole design rests on: NEVER a false negative. */
    @Test
    void everyAddedKeyIsReportedAsPossiblyPresent() {
        BloomFilter filter = BloomFilter.create(1_000, 0.01);
        for (int i = 0; i < 1_000; i++) {
            filter.add("key-" + i);
        }
        for (int i = 0; i < 1_000; i++) {
            assertTrue(filter.mightContain("key-" + i),
                    "false negative for key-" + i + " — never allowed");
        }
    }

    @Test
    void falsePositiveRateIsNearTheConfiguredTarget() {
        BloomFilter filter = BloomFilter.create(1_000, 0.01);
        for (int i = 0; i < 1_000; i++) {
            filter.add("key-" + i);
        }

        int falsePositives = 0;
        int probes = 10_000;
        for (int i = 0; i < probes; i++) {
            if (filter.mightContain("absent-" + i)) {
                falsePositives++;
            }
        }
        // Target is 1%; allow generous slack for hash variance.
        assertTrue(falsePositives < probes * 0.03,
                "false-positive rate too high: " + falsePositives + "/" + probes);
    }

    @Test
    void survivesSerializationRoundTrip(@TempDir Path dir) throws IOException {
        BloomFilter original = BloomFilter.create(500, 0.01);
        for (int i = 0; i < 500; i++) {
            original.add("key-" + i);
        }

        Path file = dir.resolve("filter.bloom");
        original.writeTo(file);
        BloomFilter loaded = BloomFilter.readFrom(file);

        for (int i = 0; i < 500; i++) {
            assertTrue(loaded.mightContain("key-" + i), "false negative after reload");
        }
        // Identical bit-array ⇒ identical answers, including negatives.
        int agreements = 0;
        for (int i = 0; i < 1_000; i++) {
            if (loaded.mightContain("absent-" + i) == original.mightContain("absent-" + i)) {
                agreements++;
            }
        }
        assertTrue(agreements == 1_000, "loaded filter diverged from original");
    }

    @Test
    void tinyFilterStillWorks() {
        BloomFilter filter = BloomFilter.create(1, 0.01);
        filter.add("only");
        assertTrue(filter.mightContain("only"));
        assertFalse(filter.mightContain("definitely-not-only"));
    }
}
