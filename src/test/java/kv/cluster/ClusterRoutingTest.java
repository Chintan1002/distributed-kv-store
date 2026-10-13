package kv.cluster;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import kv.api.HttpServer;
import kv.engine.StorageEngine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Integration: a 3-node in-process cluster routing keys via the ring. */
class ClusterRoutingTest {

    private static final List<String> NODE_IDS = List.of("n1", "n2", "n3");
    private static final Map<String, StorageEngine> engines = new HashMap<>();
    private static final Map<String, Coordinator> coordinators = new HashMap<>();
    private static final Map<String, HttpServer> servers = new HashMap<>();
    private static final Map<String, Integer> ports = new HashMap<>();
    private static final HttpClient client = HttpClient.newHttpClient();

    @BeforeAll
    static void startCluster() throws IOException {
        Map<String, String> members = new HashMap<>();
        for (String nodeId : NODE_IDS) { // reserve a free port per node
            try (ServerSocket socket = new ServerSocket(0)) {
                ports.put(nodeId, socket.getLocalPort());
                members.put(nodeId, "localhost:" + socket.getLocalPort());
            }
        }
        for (String nodeId : NODE_IDS) {
            StorageEngine engine = new StorageEngine(Files.createTempDirectory("kv-" + nodeId));
            Coordinator coordinator = new Coordinator(nodeId, members, engine);
            HttpServer server = new HttpServer(coordinator);
            server.start(ports.get(nodeId));
            engines.put(nodeId, engine);
            coordinators.put(nodeId, coordinator);
            servers.put(nodeId, server);
        }
    }

    @AfterAll
    static void stopCluster() throws IOException {
        for (String nodeId : NODE_IDS) {
            servers.get(nodeId).stop();
            engines.get(nodeId).close();
        }
    }

    private HttpResponse<String> request(String nodeId, String method, String key, String body)
            throws IOException, InterruptedException {
        URI uri = URI.create("http://localhost:" + ports.get(nodeId) + "/kv/" + key);
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri);
        HttpRequest request = switch (method) {
            case "PUT" -> builder.PUT(HttpRequest.BodyPublishers.ofString(body)).build();
            case "DELETE" -> builder.DELETE().build();
            default -> builder.GET().build();
        };
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void keysWrittenViaOneNodeAreReadableViaEveryNode() throws Exception {
        for (int i = 0; i < 60; i++) { // all writes through n1 — the ring routes them
            assertEquals(200, request("n1", "PUT", "route-key-" + i, "value-" + i).statusCode());
        }
        for (String readVia : NODE_IDS) {
            for (int i = 0; i < 60; i++) {
                HttpResponse<String> response = request(readVia, "GET", "route-key-" + i, null);
                assertEquals(200, response.statusCode(), "missing via " + readVia);
                assertEquals("value-" + i, response.body());
            }
        }
    }

    @Test
    void dataPhysicallyLivesOnlyOnTheOwnerNode() throws Exception {
        assertEquals(200, request("n2", "PUT", "physical-location-key", "here").statusCode());

        String owner = coordinators.get("n1").ring().ownerOf("physical-location-key");
        for (String nodeId : NODE_IDS) { // bypass HTTP: ask each local engine directly
            var local = engines.get(nodeId).get("physical-location-key");
            if (nodeId.equals(owner)) {
                assertEquals("here", local.orElseThrow(), "owner must hold the key locally");
            } else {
                assertTrue(local.isEmpty(), nodeId + " must not hold a key it doesn't own (no replication yet)");
            }
        }
    }

    @Test
    void everyNodeOwnsASliceOfTheKeyspace() {
        Map<String, Double> ownership = coordinators.get("n1").ring().ownershipFractions();
        for (String nodeId : NODE_IDS) {
            assertTrue(ownership.get(nodeId) > 0.2,
                    nodeId + " owns " + ownership.get(nodeId) + " — too little");
        }
    }

    @Test
    void deleteViaNonOwnerRemovesTheKeyEverywhere() throws Exception {
        assertEquals(200, request("n1", "PUT", "cluster-doomed", "bye").statusCode());
        assertEquals(200, request("n3", "DELETE", "cluster-doomed", null).statusCode());
        for (String readVia : NODE_IDS) {
            assertEquals(404, request(readVia, "GET", "cluster-doomed", null).statusCode());
        }
    }
}
