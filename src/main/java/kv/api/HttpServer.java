package kv.api;

import io.javalin.Javalin;
import java.util.Map;
import java.util.TreeMap;
import kv.cluster.Coordinator;
import kv.engine.StorageEngine;

/**
 * HTTP front door for a single node. Routes:
 * PUT/GET/DELETE /kv/{key} (routed via the coordinator), /stats
 * (local engine internals), /ring (ownership debug).
 *
 * <p>I/O failures (a WAL append that cannot fsync, an unreachable peer)
 * propagate out of the handler and surface as HTTP 500 — never swallowed,
 * never acked (invariant: an acked write is durable).
 */
public final class HttpServer {

    private final Javalin app;

    public HttpServer(Coordinator coordinator) {
        // Virtual-thread-per-request (Project Loom): thousands of concurrent
        // blocking connections without a platform-thread pool to size.
        this.app = Javalin.create(config -> config.useVirtualThreads = true);

        app.put("/kv/{key}", ctx -> {
            coordinator.put(ctx.pathParam("key"), ctx.body());
            ctx.status(200);
        });

        app.get("/kv/{key}", ctx ->
                coordinator.get(ctx.pathParam("key")).ifPresentOrElse(
                        ctx::result,
                        () -> ctx.status(404)));

        app.delete("/kv/{key}", ctx -> {
            coordinator.delete(ctx.pathParam("key"));
            ctx.status(200);
        });

        // Local engine internals; the counters show missing-key GETs
        // skipping disk reads. (Hand-built JSON: no Jackson dependency.)
        app.get("/stats", ctx -> {
            StorageEngine.StatsSnapshot stats = coordinator.localEngine().stats();
            ctx.contentType("application/json").result("""
                    {"memTableBytes":%d,"ssTableCount":%d,"bloomSkips":%d,\
                    "ssTableScans":%d,"ssTableScanMisses":%d,"bloomHitRate":%.4f,"compactions":%d}"""
                    .formatted(stats.memTableBytes(), stats.ssTableCount(), stats.bloomSkips(),
                            stats.ssTableScans(), stats.ssTableScanMisses(), stats.bloomHitRate(),
                            stats.compactions()));
        });

        // Ring ownership debug: who is on the ring and what
        // exact share of the keyspace each node owns.
        app.get("/ring", ctx -> {
            Map<String, Double> ownership = new TreeMap<>(coordinator.ring().ownershipFractions());
            StringBuilder json = new StringBuilder("{\"self\":\"").append(coordinator.selfId())
                    .append("\",\"virtualNodesPerNode\":")
                    .append(coordinator.ring().virtualNodesPerNode())
                    .append(",\"ownership\":{");
            boolean first = true;
            for (Map.Entry<String, Double> e : ownership.entrySet()) {
                if (!first) {
                    json.append(',');
                }
                json.append('"').append(e.getKey()).append("\":")
                        .append("%.4f".formatted(e.getValue()));
                first = false;
            }
            ctx.contentType("application/json").result(json.append("}}").toString());
        });
    }

    public void start(int port) {
        app.start(port);
    }

    public void stop() {
        app.stop();
    }

    /** Actual bound port (useful when started with port 0 in tests). */
    public int port() {
        return app.port();
    }
}
