package kv.ring;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsistentHashRingTest {

    private static final int VNODES = 128;
    private static final int SAMPLE_KEYS = 10_000;

    private static ConsistentHashRing ringOf(String... nodeIds) {
        ConsistentHashRing ring = new ConsistentHashRing(VNODES);
        for (String nodeId : nodeIds) {
            ring.addNode(nodeId);
        }
        return ring;
    }

    private static Map<String, String> ownersOfSampleKeys(ConsistentHashRing ring) {
        Map<String, String> owners = new HashMap<>();
        for (int i = 0; i < SAMPLE_KEYS; i++) {
            String key = "key-" + i;
            owners.put(key, ring.ownerOf(key));
        }
        return owners;
    }

    @Test
    void ownershipIsDeterministic() {
        ConsistentHashRing a = ringOf("n1", "n2", "n3");
        ConsistentHashRing b = ringOf("n3", "n1", "n2"); // insertion order must not matter
        for (int i = 0; i < 100; i++) {
            assertEquals(a.ownerOf("key-" + i), b.ownerOf("key-" + i));
        }
    }

    @Test
    void threeNodesOwnRoughlyEqualShares() {
        ConsistentHashRing ring = ringOf("n1", "n2", "n3");

        Map<String, Long> counts = new HashMap<>();
        ownersOfSampleKeys(ring).values().forEach(owner -> counts.merge(owner, 1L, Long::sum));

        assertEquals(Set.of("n1", "n2", "n3"), counts.keySet());
        for (Map.Entry<String, Long> e : counts.entrySet()) {
            double share = e.getValue() / (double) SAMPLE_KEYS;
            assertTrue(share > 0.25 && share < 0.42,
                    e.getKey() + " owns " + share + " of keys — too lumpy");
        }

        // The exact arc-length math must agree with the sampled reality.
        Map<String, Double> fractions = ring.ownershipFractions();
        double sum = fractions.values().stream().mapToDouble(Double::doubleValue).sum();
        assertEquals(1.0, sum, 1e-9, "ownership fractions must cover the whole ring");
        for (Map.Entry<String, Double> e : fractions.entrySet()) {
            assertTrue(e.getValue() > 0.25 && e.getValue() < 0.42,
                    e.getKey() + " owns fraction " + e.getValue() + " — too lumpy");
        }
    }

    @Test
    void addingANodeMovesOnlyAboutOneNthOfKeys_andOnlyToTheNewNode() {
        ConsistentHashRing ring = ringOf("n1", "n2", "n3");
        Map<String, String> before = ownersOfSampleKeys(ring);

        ring.addNode("n4");
        Map<String, String> after = ownersOfSampleKeys(ring);

        long moved = 0;
        for (String key : before.keySet()) {
            if (!before.get(key).equals(after.get(key))) {
                moved++;
                // The defining property of consistent hashing: a new node only
                // TAKES keys; keys never shuffle between existing nodes.
                assertEquals("n4", after.get(key),
                        "key moved between existing nodes — that must never happen");
            }
        }
        double movedShare = moved / (double) SAMPLE_KEYS;
        assertTrue(movedShare > 0.15 && movedShare < 0.35,
                "expected ~1/4 of keys to move, got " + movedShare);
    }

    @Test
    void removingANodeOnlyReassignsItsOwnKeys() {
        ConsistentHashRing ring = ringOf("n1", "n2", "n3");
        Map<String, String> before = ownersOfSampleKeys(ring);

        ring.removeNode("n2");
        Map<String, String> after = ownersOfSampleKeys(ring);

        for (String key : before.keySet()) {
            if (!before.get(key).equals("n2")) {
                assertEquals(before.get(key), after.get(key),
                        "a key not owned by the removed node must not move");
            } else {
                assertTrue(Set.of("n1", "n3").contains(after.get(key)));
            }
        }
    }

    @Test
    void singleNodeOwnsTheWholeRing() {
        ConsistentHashRing ring = ringOf("only");
        assertEquals("only", ring.ownerOf("anything"));

        Map<String, Double> fractions = ring.ownershipFractions();
        assertEquals(Set.of("only"), fractions.keySet());
        assertEquals(1.0, fractions.get("only"), 1e-9); // sum of 128 arcs: float tolerance
    }
}
