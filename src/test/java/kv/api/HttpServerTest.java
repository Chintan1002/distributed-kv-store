package kv.api;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import kv.cluster.Coordinator;
import kv.engine.StorageEngine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpServerTest {

    private static StorageEngine engine;
    private static HttpServer server;
    private static HttpClient client;
    private static String baseUrl;

    @BeforeAll
    static void startServer() throws IOException {
        engine = new StorageEngine(Files.createTempDirectory("kv-http-test"));
        server = new HttpServer(Coordinator.singleNode(engine));
        server.start(0); // random free port
        baseUrl = "http://localhost:" + server.port() + "/kv/";
        client = HttpClient.newHttpClient();
    }

    @AfterAll
    static void stopServer() throws IOException {
        server.stop();
        engine.close();
    }

    @Test
    void putThenGetReturnsValue() throws Exception {
        assertEquals(200, put("greeting", "hello world").statusCode());

        HttpResponse<String> get = get("greeting");
        assertEquals(200, get.statusCode());
        assertEquals("hello world", get.body());
    }

    @Test
    void overwriteReturnsLatestValue() throws Exception {
        put("counter", "1");
        put("counter", "2");
        assertEquals("2", get("counter").body());
    }

    @Test
    void missingKeyReturns404() throws Exception {
        assertEquals(404, get("no-such-key").statusCode());
    }

    @Test
    void statsEndpointReportsEngineInternals() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(
                URI.create(baseUrl.replace("/kv/", "/stats"))).GET().build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"bloomHitRate\""));
        assertTrue(response.body().contains("\"ssTableCount\""));
        assertTrue(response.body().contains("\"memTableBytes\""));
    }

    @Test
    void ringEndpointShowsSingleNodeOwningEverything() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(
                URI.create(baseUrl.replace("/kv/", "/ring"))).GET().build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"self\":\"node-1\""));
        assertTrue(response.body().contains("\"node-1\":1.0000"));
    }

    @Test
    void deleteThenGetReturns404() throws Exception {
        put("doomed", "bye");
        assertEquals(200, delete("doomed").statusCode());
        assertEquals(404, get("doomed").statusCode());
    }

    private HttpResponse<String> put(String key, String value) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + key))
                .PUT(HttpRequest.BodyPublishers.ofString(value))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String key) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + key)).GET().build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> delete(String key) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + key)).DELETE().build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
