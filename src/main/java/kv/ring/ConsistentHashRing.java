package kv.ring;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Consistent-hash ring: positions 0…2⁶⁴−1, each physical node
 * claiming many virtual positions. A key belongs to the first virtual node
 * clockwise from hash(key) — {@code ceilingEntry}, wrapping to
 * {@code firstEntry}. Adding or removing a node moves only ~1/N of the keys.
 *
 * <p>Not safe for concurrent mutation: build the membership at startup
 * (static config; no gossip yet) and treat it as read-only afterwards.
 */
public final class ConsistentHashRing {

    private final TreeMap<Long, VirtualNode> ring = new TreeMap<>();
    private final int virtualNodesPerNode;

    public ConsistentHashRing(int virtualNodesPerNode) {
        this.virtualNodesPerNode = virtualNodesPerNode;
    }

    public void addNode(String nodeId) {
        for (int i = 0; i < virtualNodesPerNode; i++) {
            VirtualNode vnode = new VirtualNode(nodeId, i);
            ring.put(vnode.position(), vnode);
        }
    }

    public void removeNode(String nodeId) {
        ring.values().removeIf(vnode -> vnode.nodeId().equals(nodeId));
    }

    /** The node that owns this key: first virtual node clockwise from hash(key). */
    public String ownerOf(String key) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("ring has no nodes");
        }
        long h = VirtualNode.hash(key);
        Map.Entry<Long, VirtualNode> entry = ring.ceilingEntry(h);
        if (entry == null) {
            entry = ring.firstEntry(); // wrap past the top of the ring
        }
        return entry.getValue().nodeId();
    }

    public Set<String> nodes() {
        return ring.values().stream().map(VirtualNode::nodeId).collect(Collectors.toSet());
    }

    public int virtualNodesPerNode() {
        return virtualNodesPerNode;
    }

    /**
     * Exact share of the 2⁶⁴ keyspace each node owns, computed from the arc
     * lengths between consecutive virtual nodes (each arc belongs to the
     * virtual node at its clockwise end). Sums to 1.0. Debug/telemetry for
     * the /ring endpoint.
     */
    public Map<String, Double> ownershipFractions() {
        Map<String, Double> fractions = new HashMap<>();
        if (ring.isEmpty()) {
            return fractions;
        }
        if (ring.size() == 1) {
            return Map.of(ring.firstEntry().getValue().nodeId(), 1.0);
        }
        long previous = ring.lastKey(); // the arc to the first position wraps around
        for (Map.Entry<Long, VirtualNode> entry : ring.entrySet()) {
            long arc = entry.getKey() - previous; // two's-complement wrap = unsigned distance
            double fraction = (arc >= 0 ? (double) arc : (double) arc + 0x1p64) / 0x1p64;
            fractions.merge(entry.getValue().nodeId(), fraction, Double::sum);
            previous = entry.getKey();
        }
        return fractions;
    }
}
