package kv.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import kv.engine.StorageEngine;
import kv.ring.ConsistentHashRing;

/**
 * Any node is the coordinator for requests it receives:
 * hash the key on the ring; if we own it, serve from the local engine,
 * otherwise forward the request to the owner over HTTP. All members share
 * the same static membership config, so every node computes the same owner
 * and a forwarded request is always served locally by its target — no
 * forwarding loops. Replication and quorums are not implemented yet.
 */
public final class Coordinator implements Closeable {

    public static final int VIRTUAL_NODES_PER_NODE = 128;

    private final String selfId;
    private final ConsistentHashRing ring;
    private final Map<String, String> memberAddresses; // nodeId -> host:port
    private final StorageEngine engine;
    // Blocking HTTP on virtual threads: simple code, scales anyway.
    private final HttpClient http = HttpClient.newBuilder()
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            .build();

    /** Standalone mode: one node that owns the whole ring, never forwards. */
    public static Coordinator singleNode(StorageEngine engine) {
        return new Coordinator("node-1", Map.of(), engine);
    }

    /** @param memberAddresses every cluster member (including self), nodeId -> host:port */
    public Coordinator(String selfId, Map<String, String> memberAddresses, StorageEngine engine) {
        this.selfId = selfId;
        this.memberAddresses = Map.copyOf(memberAddresses);
        this.engine = engine;
        this.ring = new ConsistentHashRing(VIRTUAL_NODES_PER_NODE);
        ring.addNode(selfId);
        for (String nodeId : memberAddresses.keySet()) {
            if (!nodeId.equals(selfId)) {
                ring.addNode(nodeId);
            }
        }
    }

    public void put(String key, String value) throws IOException {
        String owner = ring.ownerOf(key);
        if (owner.equals(selfId)) {
            engine.put(key, value);
            return;
        }
        HttpResponse<String> response = forward(HttpRequest.newBuilder(kvUri(owner, key))
                .PUT(HttpRequest.BodyPublishers.ofString(value)).build());
        if (response.statusCode() != 200) {
            throw new IOException("PUT forwarded to " + owner + " failed: HTTP " + response.statusCode());
        }
    }

    public Optional<String> get(String key) throws IOException {
        String owner = ring.ownerOf(key);
        if (owner.equals(selfId)) {
            return engine.get(key);
        }
        HttpResponse<String> response = forward(HttpRequest.newBuilder(kvUri(owner, key)).GET().build());
        return switch (response.statusCode()) {
            case 200 -> Optional.of(response.body());
            case 404 -> Optional.empty();
            default -> throw new IOException(
                    "GET forwarded to " + owner + " failed: HTTP " + response.statusCode());
        };
    }

    public void delete(String key) throws IOException {
        String owner = ring.ownerOf(key);
        if (owner.equals(selfId)) {
            engine.delete(key);
            return;
        }
        HttpResponse<String> response = forward(HttpRequest.newBuilder(kvUri(owner, key)).DELETE().build());
        if (response.statusCode() != 200) {
            throw new IOException("DELETE forwarded to " + owner + " failed: HTTP " + response.statusCode());
        }
    }

    private HttpResponse<String> forward(HttpRequest request) throws IOException {
        try {
            return http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while forwarding to peer", e);
        }
    }

    private URI kvUri(String nodeId, String key) {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8).replace("+", "%20");
        return URI.create("http://" + memberAddresses.get(nodeId) + "/kv/" + encodedKey);
    }

    public String selfId() {
        return selfId;
    }

    public ConsistentHashRing ring() {
        return ring;
    }

    public StorageEngine localEngine() {
        return engine;
    }

    @Override
    public void close() throws IOException {
        engine.close();
    }
}
