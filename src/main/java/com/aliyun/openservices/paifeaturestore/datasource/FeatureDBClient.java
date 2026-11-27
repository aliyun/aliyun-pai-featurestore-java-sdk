package com.aliyun.openservices.paifeaturestore.datasource;

import com.alicloud.openservices.tablestore.core.utils.IOUtils;
import com.aliyun.openservices.paifeaturestore.constants.InsertMode;
import com.google.gson.Gson;
import okhttp3.ConnectionPool;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FeatureDBClient {
    private static Log log = LogFactory.getLog(FeatureDBClient.class);
    private OkHttpClient httpclient = null;
    private String address = null;
    private String token = null;
    private String signature = null;
    private int retryCount = 3;

    static final int VPC_HEALTH_CHECK_ATTEMPTS = 3;
    static final long VPC_HEALTH_CHECK_INTERVAL_MS = 50L;
    static final int VPC_HEALTH_CHECK_CONNECT_TIMEOUT_MS = 200;
    static final int VPC_HEALTH_CHECK_TIMEOUT_MS = 500;

    // 创建一个全局Gson实例
    private static final Gson gson = new Gson();
    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    public FeatureDBClient(HttpConfig httpConfig) {
        try {
            OkHttpClient.Builder builder = new OkHttpClient.Builder();
            builder.connectTimeout(httpConfig.getConnectTimeout(), TimeUnit.MILLISECONDS);
            builder.readTimeout(httpConfig.getReadTimeout(), TimeUnit.MILLISECONDS);
            builder.writeTimeout(httpConfig.getWriteTimeout(), TimeUnit.MILLISECONDS);
            builder.socketFactory(new SocketFactory() {

                @Override
                public Socket createSocket() throws IOException {
                    Socket socket = new Socket();
                    socket.setTcpNoDelay(true);
                    socket.setReuseAddress(true);
                    socket.setSoTimeout(httpConfig.getReadTimeout());
                    socket.setKeepAlive(httpConfig.isKeepAlive());
                    return socket;
                }

                @Override
                public Socket createSocket(String s, int i) throws IOException, UnknownHostException {
                    Socket socket = new Socket(s, i);
                    socket.setTcpNoDelay(true);
                    socket.setReuseAddress(true);
                    socket.setSoTimeout(httpConfig.getReadTimeout());
                    socket.setKeepAlive(httpConfig.isKeepAlive());
                    return socket;
                }

                @Override
                public Socket createSocket(String s, int i, InetAddress inetAddress, int i1) throws IOException, UnknownHostException {
                    Socket socket = new Socket(s, i, inetAddress, i1);
                    socket.setTcpNoDelay(true);
                    socket.setReuseAddress(true);
                    socket.setSoTimeout(httpConfig.getReadTimeout());
                    socket.setKeepAlive(httpConfig.isKeepAlive());
                    return socket;
                }

                @Override
                public Socket createSocket(InetAddress inetAddress, int i) throws IOException {
                    Socket socket = new Socket( inetAddress, i);
                    socket.setTcpNoDelay(true);
                    socket.setReuseAddress(true);
                    socket.setSoTimeout(httpConfig.getReadTimeout());
                    socket.setKeepAlive(httpConfig.isKeepAlive());
                    return socket;
                }

                @Override
                public Socket createSocket(InetAddress inetAddress, int i, InetAddress inetAddress1, int i1) throws IOException {
                    Socket socket = new Socket( inetAddress, i, inetAddress1, i1);
                    socket.setTcpNoDelay(true);
                    socket.setReuseAddress(true);
                    socket.setSoTimeout(httpConfig.getReadTimeout());
                    socket.setKeepAlive(httpConfig.isKeepAlive());
                    return socket;
                }
            });

            builder.connectionPool(new ConnectionPool(1000, 30, TimeUnit.MINUTES));
            httpclient = builder.build();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public OkHttpClient getHttpclient() {
        return httpclient;
    }

    public void setHttpclient(OkHttpClient httpclient) {
        this.httpclient = httpclient;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    /**
     * Probes the PrivateLink /health endpoint during initialization and switches
     * the client address to PVL when healthy, keeping the original address otherwise.
     */
    public boolean selectVpcAddress(String vpcAddress) {
        String normalizedAddress = normalizeVpcAddress(vpcAddress);
        if (normalizedAddress == null) {
            log.warn("FeatureDB uses normal address because PVL address is empty or invalid");
            return false;
        }
        if (this.httpclient == null) {
            log.warn("FeatureDB uses normal address because HTTP client is not initialized");
            return false;
        }

        String healthUrl = normalizedAddress + "/health";
        Request request;
        try {
            request = new Request.Builder()
                    .url(healthUrl)
                    .get()
                    .addHeader("Content-Type", "application/json")
                    .build();
        } catch (IllegalArgumentException e) {
            log.warn(String.format("FeatureDB uses normal address because PVL address is invalid (%s): %s",
                    vpcAddress, e.getMessage()));
            return false;
        }

        OkHttpClient healthClient = this.httpclient.newBuilder()
                .connectTimeout(VPC_HEALTH_CHECK_CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .readTimeout(VPC_HEALTH_CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .writeTimeout(VPC_HEALTH_CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .callTimeout(VPC_HEALTH_CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .retryOnConnectionFailure(false)
                .build();

        String lastError = null;
        for (int attempt = 0; attempt < VPC_HEALTH_CHECK_ATTEMPTS; attempt++) {
            boolean retryable;
            try (Response response = healthClient.newCall(request).execute()) {
                int statusCode = response.code();
                if (statusCode != 200 && statusCode < 500) {
                    lastError = "status code: " + statusCode;
                    retryable = false;
                } else {
                    if (response.body() != null) {
                        drainResponseBody(response.body().byteStream());
                    }
                    if (statusCode == 200) {
                        this.address = normalizedAddress;
                        log.info("FeatureDB uses PVL address: " + normalizedAddress);
                        return true;
                    }
                    lastError = "status code: " + statusCode;
                    retryable = true;
                }
            } catch (IOException e) {
                lastError = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                retryable = true;
            }

            if (!retryable || attempt == VPC_HEALTH_CHECK_ATTEMPTS - 1) {
                break;
            }
            try {
                Thread.sleep(VPC_HEALTH_CHECK_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                lastError = "interrupted during PVL health check";
                break;
            }
        }

        log.warn(String.format("FeatureDB uses normal address because PVL health check failed (%s): %s",
                healthUrl, lastError));
        return false;
    }

    static String normalizeVpcAddress(String rawAddress) {
        if (rawAddress == null) {
            return null;
        }
        String normalized = rawAddress.trim();
        if (normalized.isEmpty()) {
            return null;
        }
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        if (normalized.isEmpty() || "http:".equalsIgnoreCase(normalized) || "https:".equalsIgnoreCase(normalized)) {
            return null;
        }
        String lower = normalized.toLowerCase(Locale.ROOT);
        if (!lower.startsWith("http://") && !lower.startsWith("https://")) {
            normalized = "http://" + normalized;
        }
        return normalized;
    }

    private static void drainResponseBody(InputStream inputStream) throws IOException {
        byte[] buffer = new byte[1024];
        while (inputStream.read(buffer) != -1) {
            // Drain body so connections can be reused and truncated responses fail the check.
        }
    }

    public byte[] requestFeatureDB(List<String> keys, String database, String schema, String table) throws Exception {
        String url = String.format("%s/api/v1/tables/%s/%s/%s/batch_get_kv2?batch_size=%d&encoder=", address, database, schema, table, keys.size());
        Map<String, Object> map = new HashMap<>();
        map.put("keys", keys);
        String requestBody = gson.toJson(map);
        RequestBody body = RequestBody.create(requestBody, JSON);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .addHeader("Authorization", token)
                .addHeader("Auth", signature)
                .build();

        byte[] content = null;

        for (int i = 0; i < retryCount ; i++) {
            try {
                content = this.doRequest(request);
                break;
            } catch(HttpException e) {
                int statusCode = e.getCode();
                String errorMessage = String.format("URL: %s, code: %d, error: %s", url, statusCode, e.getMessage());
                if ( i < retryCount -1 ) {
                    log.debug(errorMessage);
                } else {
                    log.error(errorMessage);
                    throw e;
                }
            } catch (Exception e) {
                String errorMessage = String.format("URL: %s, error: %s", url, e.getMessage());
                if (i < retryCount -1 ) {
                    log.debug(errorMessage);
                } else {
                    log.error(errorMessage);
                    throw e;
                }
            }
        }
        return content;

    }

    public byte[] doRequest(Request request) throws HttpException, IOException {
        try (Response response  = httpclient.newCall(request).execute()){
            if (response.isSuccessful() && response.body() != null) {
                 try(InputStream inputStream = response.body().byteStream()) {
                     byte[] sizeByte = new byte[4];
                     if (inputStream.read(sizeByte) != sizeByte.length) {
                         throw new HttpException(-1, "input stream read error");
                     }

                     int size = ByteBuffer.wrap(sizeByte).order(ByteOrder.LITTLE_ENDIAN).getInt();
                     byte[] content = new byte[size];
                     int offset = 0;
                     while (offset < size) {
                         int read = inputStream.read(content, offset, size - offset);
                         if (read == -1) { // End of the stream
                             throw new HttpException(-1, "Input stream read error: unexpected end of stream");
                         }
                         offset += read;
                     }

                     return content;

                 }
            } else {
                int errorCode = response.code();
                try (InputStream errorStream = response.body().byteStream()) {
                    String errorMessage = IOUtils.readStreamAsString(errorStream, "UTF-8");
                    if (errorMessage == null) {
                        errorMessage = String.format("Error code: %d", errorCode);
                    }
                    throw new HttpException(errorCode, errorMessage);
                }
            }
        }
    }
    public void writeFeatureDB(List<Map<String, Object>> data, String database, String schema, String table) throws Exception {
        InsertMode insertMode = InsertMode.Unknown;
        for (Map<String, Object> item : data) {
            if (item.containsKey("__insert_mode__")) {
                insertMode = (InsertMode) item.get("__insert_mode__");
                item.remove("__insert_mode__");
            }
        }
        String url = String.format("%s/api/v1/tables/%s/%s/%s/write", address, database, schema, table );
        Map<String, Object> map = new HashMap<>();
        map.put("content", data);
        map.put("write_mode", insertMode);
        String requestBody = gson.toJson(map);
        RequestBody body = RequestBody.create(requestBody, JSON);
        // 请求只构造一次:上面已经把 __insert_mode__ 从 data 里移除了,重试时无法再解析出 write_mode
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .addHeader("Authorization", token)
                .addHeader("Auth", signature)
                .build();

        // retryCount 可能被外部设成 0,至少要尝试一次,否则数据会被静默丢弃
        int maxAttempts = Math.max(1, retryCount);
        for (int i = 0; i < maxAttempts; i++) {
            try {
                this.doWriteRequest(request);
                return;
            } catch (HttpException e) {
                String errorMessage = String.format("URL: %s, code: %d, error: %s", url, e.getCode(), e.getMessage());
                // 4xx 是请求本身的问题,重试也不会成功
                if (i >= maxAttempts - 1 || !isRetryableStatusCode(e.getCode())) {
                    log.error(errorMessage);
                    throw e;
                }
                log.debug(errorMessage);
            } catch (IOException e) {
                String errorMessage = String.format("URL: %s, error: %s", url, e.getMessage());
                if (i >= maxAttempts - 1) {
                    log.error(errorMessage);
                    throw e;
                }
                log.debug(errorMessage);
            }
        }
    }

    // 非 private 以便测试注入失败,验证重试行为
    protected void doWriteRequest(Request request) throws HttpException, IOException {
        try(Response response = this.httpclient.newCall(request).execute()) {
           if (!response.isSuccessful() || response.code() != 200) {
               int errorCode = response.code();
               try (InputStream errorStream = response.body().byteStream()) {
                   String errorMessage = IOUtils.readStreamAsString(errorStream, "UTF-8");
                   throw new HttpException(errorCode, errorMessage);
               }
           }
        }
    }

    // 限流和服务端错误可以重试,其余状态码重试也不会成功
    private static boolean isRetryableStatusCode(int statusCode) {
        return statusCode == 429 || statusCode >= 500;
    }
}
