package com.aliyun.openservices.paifeaturestore.datasource;

import com.aliyun.openservices.paifeaturestore.api.ApiClient;
import com.aliyun.openservices.paifeaturestore.api.Configuration;
import com.aliyun.openservices.paifeaturestore.api.DatasourceApi;
import com.aliyun.openservices.paifeaturestore.constants.DatasourceType;
import com.aliyun.openservices.paifeaturestore.constants.InsertMode;
import com.aliyun.openservices.paifeaturestore.domain.Project;
import com.aliyun.openservices.paifeaturestore.model.Datasource;
import com.aliyun.paifeaturestore20230621.Client;
import com.aliyun.paifeaturestore20230621.models.GetDatasourceResponse;
import com.aliyun.paifeaturestore20230621.models.GetDatasourceResponseBody;
import com.sun.net.httpserver.HttpServer;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for FeatureDB PrivateLink (PVL) health check selection, fallback,
 * Datasource generation, and DatasourceApi fdb_vpc_plk_address parsing.
 */
public class FeatureDBClientPrivateLinkTest {

    private static final String NORMAL_ADDRESS = "http://normal.example.com";
    private static final String PUBLIC_ADDRESS = "http://public.example.com";

    private static HttpServer startServer(int[] healthStatuses, AtomicInteger healthChecks) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/health", exchange -> {
            int idx = healthChecks.getAndIncrement();
            int status = healthStatuses[Math.min(idx, healthStatuses.length - 1)];
            byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
        return server;
    }

    private static String hostPort(HttpServer server) {
        return "127.0.0.1:" + server.getAddress().getPort();
    }

    @Test
    public void testSelectVpcAddressHealthyRoutesReadsAndWritesToPvl() throws Exception {
        AtomicInteger healthChecks = new AtomicInteger();
        AtomicInteger readRequests = new AtomicInteger();
        AtomicInteger writeRequests = new AtomicInteger();
        AtomicReference<String> healthContentType = new AtomicReference<>();
        AtomicReference<String> healthMethod = new AtomicReference<>();

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/health", exchange -> {
            healthChecks.incrementAndGet();
            healthMethod.set(exchange.getRequestMethod());
            healthContentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.createContext("/api/v1/tables/db/schema/tbl/batch_get_kv2", exchange -> {
            readRequests.incrementAndGet();
            byte[] payload = new byte[]{1, 2, 3, 4};
            ByteBuffer framed = ByteBuffer.allocate(4 + payload.length).order(ByteOrder.LITTLE_ENDIAN);
            framed.putInt(payload.length);
            framed.put(payload);
            byte[] resp = framed.array();
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(resp);
            }
        });
        server.createContext("/api/v1/tables/db/schema/tbl/write", exchange -> {
            writeRequests.incrementAndGet();
            byte[] resp = "ok".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(resp);
            }
        });
        server.start();

        try {
            FeatureDBClient client = new FeatureDBClient(new HttpConfig());
            client.setAddress(NORMAL_ADDRESS);
            client.setToken("test-token");
            client.setSignature("test-signature");

            String pvlHostPort = hostPort(server);
            assertTrue(client.selectVpcAddress(pvlHostPort));
            assertEquals("http://" + pvlHostPort, client.getAddress());
            assertEquals(1, healthChecks.get());
            assertEquals("GET", healthMethod.get());
            assertEquals("application/json", healthContentType.get());

            byte[] readContent = client.requestFeatureDB(Collections.singletonList("k1"), "db", "schema", "tbl");
            assertNotNull(readContent);
            assertEquals(4, readContent.length);
            assertEquals(1, readRequests.get());

            Map<String, Object> row = new HashMap<>();
            row.put("id", "1");
            row.put("__insert_mode__", InsertMode.FullRowWrite);
            client.writeFeatureDB(Collections.singletonList(row), "db", "schema", "tbl");
            assertEquals(1, writeRequests.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void testSelectVpcAddressRecoversAfterTransient5xx() throws Exception {
        AtomicInteger healthChecks = new AtomicInteger();
        HttpServer server = startServer(new int[]{503, 503, 200}, healthChecks);
        try {
            FeatureDBClient client = new FeatureDBClient(new HttpConfig());
            client.setAddress(NORMAL_ADDRESS);

            String pvlHostPort = hostPort(server);
            assertTrue(client.selectVpcAddress(pvlHostPort));
            assertEquals("http://" + pvlHostPort, client.getAddress());
            assertEquals(3, healthChecks.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void testSelectVpcAddressDoesNotRetry4xx() throws Exception {
        AtomicInteger healthChecks = new AtomicInteger();
        HttpServer server = startServer(new int[]{403}, healthChecks);
        try {
            FeatureDBClient client = new FeatureDBClient(new HttpConfig());
            client.setAddress(NORMAL_ADDRESS);

            assertFalse(client.selectVpcAddress(hostPort(server)));
            assertEquals(NORMAL_ADDRESS, client.getAddress());
            assertEquals(1, healthChecks.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void testSelectVpcAddressFallsBackAfter5xxExhausted() throws Exception {
        AtomicInteger healthChecks = new AtomicInteger();
        HttpServer server = startServer(new int[]{503}, healthChecks);
        try {
            FeatureDBClient client = new FeatureDBClient(new HttpConfig());
            client.setAddress(NORMAL_ADDRESS);

            assertFalse(client.selectVpcAddress(hostPort(server)));
            assertEquals(NORMAL_ADDRESS, client.getAddress());
            assertEquals(FeatureDBClient.VPC_HEALTH_CHECK_ATTEMPTS, healthChecks.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void testSelectVpcAddressFallsBackOnIncompleteBody() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        try (ServerSocket serverSocket = new ServerSocket(0, 16, java.net.InetAddress.getByName("127.0.0.1"))) {
            Thread acceptThread = new Thread(() -> {
                while (!serverSocket.isClosed()) {
                    try (Socket socket = serverSocket.accept()) {
                        requests.incrementAndGet();
                        byte[] buf = new byte[512];
                        socket.getInputStream().read(buf);
                        // Claim Content-Length: 10 but write only 5 bytes and close.
                        String truncated = "HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\nshort";
                        socket.getOutputStream().write(truncated.getBytes(StandardCharsets.US_ASCII));
                        socket.getOutputStream().flush();
                    } catch (IOException ignored) {
                        // Server socket closed on cleanup.
                    }
                }
            });
            acceptThread.setDaemon(true);
            acceptThread.start();

            FeatureDBClient client = new FeatureDBClient(new HttpConfig());
            client.setAddress(NORMAL_ADDRESS);

            assertFalse(client.selectVpcAddress("127.0.0.1:" + serverSocket.getLocalPort()));
            assertEquals(NORMAL_ADDRESS, client.getAddress());
            assertEquals(FeatureDBClient.VPC_HEALTH_CHECK_ATTEMPTS, requests.get());
        }
    }

    @Test
    public void testSelectVpcAddressTimeoutFallsBackAndPreservesClientTimeouts() throws Exception {
        AtomicInteger healthChecks = new AtomicInteger();
        CountDownLatch releaseServer = new CountDownLatch(1);
        ExecutorService executor = Executors.newCachedThreadPool();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(executor);
        server.createContext("/health", exchange -> {
            healthChecks.incrementAndGet();
            try {
                releaseServer.await(3, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            exchange.close();
        });
        server.start();

        try {
            HttpConfig httpConfig = new HttpConfig();
            FeatureDBClient client = new FeatureDBClient(httpConfig);
            client.setAddress(NORMAL_ADDRESS);

            long startNanos = System.nanoTime();
            assertFalse(client.selectVpcAddress(hostPort(server)));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            assertEquals(NORMAL_ADDRESS, client.getAddress());
            assertEquals(FeatureDBClient.VPC_HEALTH_CHECK_ATTEMPTS, healthChecks.get());
            assertTrue("Health check timeouts should remain bounded, took " + elapsedMs + "ms", elapsedMs < 5000);
            assertEquals("Business client read timeout must not change",
                    httpConfig.getReadTimeout(), client.getHttpclient().readTimeoutMillis());
            assertEquals("Business client connect timeout must not change",
                    httpConfig.getConnectTimeout(), client.getHttpclient().connectTimeoutMillis());
        } finally {
            releaseServer.countDown();
            server.stop(0);
            executor.shutdownNow();
        }
    }

    @Test
    public void testSelectVpcAddressHandlesExistingSchemeTrailingSlashAndInvalidInput() throws Exception {
        AtomicInteger healthChecks = new AtomicInteger();
        HttpServer server = startServer(new int[]{200}, healthChecks);
        try {
            FeatureDBClient client = new FeatureDBClient(new HttpConfig());
            client.setAddress(NORMAL_ADDRESS);

            String expectedUrl = "http://" + hostPort(server);
            assertTrue(client.selectVpcAddress(expectedUrl + "/"));
            assertEquals(expectedUrl, client.getAddress());
            assertEquals(1, healthChecks.get());

            FeatureDBClient blankClient = new FeatureDBClient(new HttpConfig());
            blankClient.setAddress(NORMAL_ADDRESS);
            assertFalse(blankClient.selectVpcAddress("   "));
            assertEquals(NORMAL_ADDRESS, blankClient.getAddress());

            FeatureDBClient invalidClient = new FeatureDBClient(new HttpConfig());
            invalidClient.setAddress(NORMAL_ADDRESS);
            assertFalse(invalidClient.selectVpcAddress("http://invalid host:99999"));
            assertEquals(NORMAL_ADDRESS, invalidClient.getAddress());

            // Bare schemes must be rejected immediately without network probes.
            assertNull(FeatureDBClient.normalizeVpcAddress("http://"));
            assertNull(FeatureDBClient.normalizeVpcAddress("https://"));
            assertNull(FeatureDBClient.normalizeVpcAddress("http:///"));
            assertEquals("https://pvl.example.com:8443",
                    FeatureDBClient.normalizeVpcAddress("https://pvl.example.com:8443/"));
            assertFalse(invalidClient.selectVpcAddress("http://"));
            assertEquals(NORMAL_ADDRESS, invalidClient.getAddress());
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void testSelectVpcAddressFallsBackOnConnectionRefused() throws Exception {
        int unusedPort;
        try (ServerSocket ss = new ServerSocket(0, 1, java.net.InetAddress.getByName("127.0.0.1"))) {
            unusedPort = ss.getLocalPort();
        }
        FeatureDBClient client = new FeatureDBClient(new HttpConfig());
        client.setAddress(NORMAL_ADDRESS);

        assertFalse(client.selectVpcAddress("127.0.0.1:" + unusedPort));
        assertEquals(NORMAL_ADDRESS, client.getAddress());
    }

    @Test
    public void testProjectUsesConfigurationUsePrivateLinkOnConstructorAndRegister() throws Exception {
        AtomicInteger healthChecks = new AtomicInteger();
        HttpServer server = startServer(new int[]{200}, healthChecks);
        try {
            String pvlHostPort = hostPort(server);
            String expectedPvlUrl = "http://" + pvlHostPort;

            Configuration cfg = new Configuration("cn-hangzhou", "ak", "sk", "proj_pvl");
            cfg.setUsePrivateLink(true);
            ApiClient apiClient = new ApiClient(cfg);

            String onlineDsName = "test_pvl_online_ds_" + System.nanoTime();
            Datasource onlineDs = new Datasource();
            onlineDs.setName(onlineDsName);
            onlineDs.setType(DatasourceType.Datasource_Type_FeatureDB);
            onlineDs.setVpcAddress(NORMAL_ADDRESS);
            onlineDs.setFdbVpcAddress(pvlHostPort);
            onlineDs.setToken("tok");

            com.aliyun.openservices.paifeaturestore.model.Project modelProject =
                    new com.aliyun.openservices.paifeaturestore.model.Project();
            modelProject.setProjectId(1L);
            modelProject.setProjectName("proj_pvl");
            modelProject.setOnlineDatasourceType(DatasourceType.Datasource_Type_FeatureDB);
            modelProject.setOnlineDataSource(onlineDs);
            modelProject.createSignature("user", "pass");

            Project domainProject = new Project(modelProject, false, apiClient);
            FeatureDBClient onlineClient = FeatureDBFactory.get(onlineDsName);
            assertNotNull(onlineClient);
            assertEquals(expectedPvlUrl, onlineClient.getAddress());
            assertEquals(1, healthChecks.get());

            String extraDsName = "test_pvl_extra_ds_" + System.nanoTime();
            Datasource extraDs = new Datasource();
            extraDs.setName(extraDsName);
            extraDs.setType(DatasourceType.Datasource_Type_FeatureDB);
            extraDs.setVpcAddress(NORMAL_ADDRESS);
            extraDs.setFdbVpcAddress(pvlHostPort);
            extraDs.setToken("tok");

            domainProject.registerFeatrueDB(extraDs);
            FeatureDBClient extraClient = FeatureDBFactory.get(extraDsName);
            assertNotNull(extraClient);
            assertEquals(expectedPvlUrl, extraClient.getAddress());
            assertEquals(2, healthChecks.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void testDatasourceGenerateFeatureDBClientRoutingModes() throws Exception {
        AtomicInteger healthChecks = new AtomicInteger();
        HttpServer server = startServer(new int[]{200}, healthChecks);
        try {
            String pvlHostPort = hostPort(server);
            Datasource ds = new Datasource();
            ds.setVpcAddress(NORMAL_ADDRESS);
            ds.setPublicAddress(PUBLIC_ADDRESS);
            ds.setFdbVpcAddress(pvlHostPort);
            ds.setToken("tok");

            // Disabled by default: never probes PVL.
            FeatureDBClient defaultClient = ds.generateFeatureDBClient(false);
            assertEquals(NORMAL_ADDRESS, defaultClient.getAddress());
            assertEquals(0, healthChecks.get());

            // Explicitly disabled: never probes PVL.
            FeatureDBClient disabledClient = ds.generateFeatureDBClient(false, false);
            assertEquals(NORMAL_ADDRESS, disabledClient.getAddress());
            assertEquals(0, healthChecks.get());

            // Public address takes precedence over PrivateLink.
            FeatureDBClient publicClient = ds.generateFeatureDBClient(true, true);
            assertEquals(PUBLIC_ADDRESS, publicClient.getAddress());
            assertEquals(0, healthChecks.get());

            // Enabled with missing PVL address: stays on normal address without probing.
            Datasource dsNoPvl = new Datasource();
            dsNoPvl.setVpcAddress(NORMAL_ADDRESS);
            dsNoPvl.setPublicAddress(PUBLIC_ADDRESS);
            dsNoPvl.setToken("tok");
            FeatureDBClient noPvlClient = dsNoPvl.generateFeatureDBClient(false, true);
            assertEquals(NORMAL_ADDRESS, noPvlClient.getAddress());
            assertEquals(0, healthChecks.get());

            // Enabled with healthy PVL address: probes and switches to PVL.
            FeatureDBClient pvlClient = ds.generateFeatureDBClient(false, true);
            assertEquals("http://" + pvlHostPort, pvlClient.getAddress());
            assertEquals(1, healthChecks.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void testDatasourceApiParsesFdbVpcPlkAddress() throws Exception {
        Datasource withPlk = fetchFeatureDBDatasource(
                "{\"fdb_vpc_address\":\"http://eas.vpc\",\"fdb_public_address\":\"http://eas.pub\",\"token\":\"t1\",\"fdb_vpc_plk_address\":\"10.0.0.1:8080\"}");
        assertEquals("10.0.0.1:8080", withPlk.getFdbVpcAddress());
        assertEquals("http://eas.vpc", withPlk.getVpcAddress());

        Datasource missingPlk = fetchFeatureDBDatasource(
                "{\"fdb_vpc_address\":\"http://eas.vpc\",\"fdb_public_address\":\"http://eas.pub\",\"token\":\"t1\"}");
        assertNull(missingPlk.getFdbVpcAddress());

        Datasource nullPlk = fetchFeatureDBDatasource(
                "{\"fdb_vpc_address\":\"http://eas.vpc\",\"fdb_public_address\":\"http://eas.pub\",\"token\":\"t1\",\"fdb_vpc_plk_address\":null}");
        assertNull(nullPlk.getFdbVpcAddress());

        Datasource blankPlk = fetchFeatureDBDatasource(
                "{\"fdb_vpc_address\":\"http://eas.vpc\",\"fdb_public_address\":\"http://eas.pub\",\"token\":\"t1\",\"fdb_vpc_plk_address\":\"  \"}");
        assertNull(blankPlk.getFdbVpcAddress());
    }

    private static Datasource fetchFeatureDBDatasource(final String configJson) throws Exception {
        Configuration cfg = new Configuration("cn-hangzhou", "ak", "sk", "proj");
        final Client stubSdkClient = new Client(cfg.getConfig()) {
            @Override
            public GetDatasourceResponse getDatasource(String instanceId, String datasourceId) {
                GetDatasourceResponseBody body = new GetDatasourceResponseBody();
                body.name = "fdb_ds";
                body.type = "FeatureDB";
                body.config = configJson;
                GetDatasourceResponse response = new GetDatasourceResponse();
                response.body = body;
                return response;
            }
        };
        ApiClient stubApiClient = new ApiClient(cfg) {
            @Override
            public Client getClient() {
                return stubSdkClient;
            }
        };
        stubApiClient.setInstanceId("fs-inst");
        return new DatasourceApi(stubApiClient).getDatasourceById(1);
    }
}
