package com.aliyun.openservices.paifeaturestore.datasource;

import com.alicloud.openservices.tablestore.core.utils.IOUtils;
import com.aliyun.openservices.paifeaturestore.constants.InsertMode;
import com.google.gson.Gson;
import okhttp3.Call;
import okhttp3.Callback;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class FeatureDBClient {
    private static Log log = LogFactory.getLog(FeatureDBClient.class);
    private OkHttpClient httpclient = null;
    private String address=null;
    private String token = null;
    private String vpcAddress = null;
    private String signature = null;
    private int retryCount = 3;

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

    public String getVpcAddress() {
        return vpcAddress;
    }

    public void setVpcAddress(String vpcAddress) {
        this.vpcAddress = vpcAddress;
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

    public Boolean CheckVpcAddress(){
        String url=String.format("%s/health",this.vpcAddress);
        Request request = new Request.Builder()
                .url(url)
                .get()
                .build();
        try {
            return httpclient.newCall(request).execute().isSuccessful();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] requestFeatureDB(List<String> keys, String database, String schema, String table) throws Exception {
        String onlineAddress = address;
        if (this.getVpcAddress()!= null && !this.getVpcAddress().isEmpty()){
            onlineAddress = this.getVpcAddress();
        }
        String url = String.format("%s/api/v1/tables/%s/%s/%s/batch_get_kv2?batch_size=%d&encoder=",
                onlineAddress, database, schema, table, keys.size());
        Map<String, Object> map = new HashMap<>();
        map.put("keys", keys);
        String requestBody = gson.toJson(map);
        RequestBody body = RequestBody.create(JSON, requestBody);
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
                //content = this.doRequestAsync(request).get();
                break;
            } catch(HttpException e) {
                //HttpException e = (HttpException) e1.getCause();
                int statusCode = e.getCode();
                String errorMessage = String.format("URL: %s, code: %d, error: %s", url, statusCode, e.getMessage());
                if ( i < retryCount) {
                    log.debug(errorMessage);
                } else {
                    log.error(errorMessage);
                    throw e;
                }
            } catch (Exception e) {
                String errorMessage = String.format("URL: %s, error: %s", url, e.getMessage());
                if (i < retryCount) {
                    log.debug(errorMessage);
                } else {
                    log.error(errorMessage);
                    throw e;
                }
            }
        }
        return content;

    }
    public byte[] kkvRequestFeatureDB(List<String> pks, String database, String schema, String table, int length) throws Exception {
        String onlineAddress = address;
        if (this.getVpcAddress()!= null && !this.getVpcAddress().isEmpty()){
            onlineAddress = this.getVpcAddress();
        }
        String url = String.format("%s/api/v1/tables/%s/%s/%s/batch_get_kkv", onlineAddress, database, schema, table);
        Map<String, Object> map = new HashMap<>();
        map.put("pks", pks);
        map.put("length", length);
        String requestBody = gson.toJson(map);
        RequestBody body = RequestBody.create(JSON, requestBody);
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
                if ( i < retryCount) {
                    log.debug(errorMessage);
                } else {
                    log.error(errorMessage);
                    throw e;
                }
            } catch (Exception e) {
                String errorMessage = String.format("URL: %s, error: %s", url, e.getMessage());
                if (i < retryCount) {
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
                    throw new HttpException(errorCode, errorMessage);
                }
            }
        }
    }
    public CompletableFuture<byte[]> doRequestAsync(Request request) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        httpclient.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                // 处理失败
                future.completeExceptionally(new HttpException(-1, e.getMessage()));
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                // 处理响应
                if (response.isSuccessful() && response.body() != null) {
                    try (InputStream inputStream = response.body().byteStream()) {
                        byte[] sizeByte = new byte[4];
                        if (inputStream.read(sizeByte) != sizeByte.length) {
                            future.completeExceptionally(new HttpException(-1, "input stream read error"));
                            return;
                        }
                        int size = ByteBuffer.wrap(sizeByte).order(ByteOrder.LITTLE_ENDIAN).getInt();
                        byte[] content = new byte[size];
                        int offset = 0;
                        while (offset < size) {
                            int read = inputStream.read(content, offset, size - offset);
                            if (read == -1) { // End of the stream
                                future.completeExceptionally(new HttpException(-1, "Input stream read error: unexpected end of stream"));
                                return;
                            }
                            offset += read;
                        }
                        future.complete(content);
                    }
                } else {
                    int errorCode = response.code();
                    try (InputStream errorStream = response.body().byteStream()) {
                        String errorMessage = IOUtils.readStreamAsString(errorStream, "UTF-8");
                        future.completeExceptionally(new HttpException(errorCode, errorMessage));
                    }
                }
            }
        });
        return future;
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
        RequestBody body = RequestBody.create(JSON, requestBody);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .addHeader("Authorization", token)
                .addHeader("Auth", signature)
                .build();

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
    public void close() throws Exception {
        if (null != this.httpclient) {
            this.httpclient.connectionPool().evictAll();
        }
    }




}
