package com.aliyun.openservices.paifeaturestore.datasource;

import com.aliyun.openservices.paifeaturestore.constants.InsertMode;
import okhttp3.Request;
import okio.Buffer;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** 验证 FeatureDBClient.writeFeatureDB 的重试行为，通过覆盖 doWriteRequest 注入失败，不依赖网络。 */
public class FeatureDBClientRetryTest {

    private static final int SUCCESS = 200;
    private static final int IO_ERROR = 0;

    /** 按预设结果依次响应每一次尝试，并记录每次尝试实际发出的请求体 */
    private static class StubFeatureDBClient extends FeatureDBClient {
        private final int[] attemptResults;
        private final List<String> attemptBodies = new ArrayList<>();

        StubFeatureDBClient(int... attemptResults) {
            super(new HttpConfig());
            this.attemptResults = attemptResults;
            // 这些只用于构造请求，doWriteRequest 被覆盖后不会真正发起网络调用
            setAddress("http://localhost:8080");
            setToken("test-token");
            setSignature("test-signature");
        }

        @Override
        protected void doWriteRequest(Request request) throws HttpException, IOException {
            attemptBodies.add(readBody(request));
            int index = attemptBodies.size() - 1;
            int result = index < attemptResults.length ? attemptResults[index] : SUCCESS;
            if (result == IO_ERROR) {
                throw new IOException("simulated network error");
            }
            if (result != SUCCESS) {
                throw new HttpException(result, "simulated error " + result);
            }
        }

        private static String readBody(Request request) throws IOException {
            Buffer buffer = new Buffer();
            request.body().writeTo(buffer);
            return buffer.readUtf8();
        }
    }

    private static List<Map<String, Object>> newData(InsertMode insertMode) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", "user_1");
        row.put("age", 30);
        if (insertMode != null) {
            row.put("__insert_mode__", insertMode);
        }
        List<Map<String, Object>> data = new ArrayList<>();
        data.add(row);
        return data;
    }

    private static void write(FeatureDBClient client, List<Map<String, Object>> data) throws Exception {
        client.writeFeatureDB(data, "db", "schema", "table");
    }

    @Test
    public void retriesServerErrorThenSucceeds() throws Exception {
        StubFeatureDBClient client = new StubFeatureDBClient(503, SUCCESS);
        write(client, newData(InsertMode.FullRowWrite));
        assertEquals("503 应该被重试一次后成功", 2, client.attemptBodies.size());
    }

    @Test
    public void retriesIoError() throws Exception {
        StubFeatureDBClient client = new StubFeatureDBClient(IO_ERROR, IO_ERROR, SUCCESS);
        write(client, newData(InsertMode.FullRowWrite));
        assertEquals("网络异常应该被重试", 3, client.attemptBodies.size());
    }

    @Test
    public void retriesTooManyRequests() throws Exception {
        StubFeatureDBClient client = new StubFeatureDBClient(429, SUCCESS);
        write(client, newData(InsertMode.FullRowWrite));
        assertEquals("429 限流应该被重试", 2, client.attemptBodies.size());
    }

    @Test
    public void doesNotRetryClientError() throws Exception {
        StubFeatureDBClient client = new StubFeatureDBClient(400, SUCCESS);
        try {
            write(client, newData(InsertMode.FullRowWrite));
            fail("4xx 应该直接抛出");
        } catch (HttpException e) {
            assertEquals(400, e.getCode());
        }
        assertEquals("4xx 不该重试", 1, client.attemptBodies.size());
    }

    @Test
    public void throwsAfterRetriesExhausted() throws Exception {
        StubFeatureDBClient client = new StubFeatureDBClient(503, 503, 503);
        try {
            write(client, newData(InsertMode.FullRowWrite));
            fail("重试用尽后应该抛出，不能静默丢数据");
        } catch (HttpException e) {
            assertEquals(503, e.getCode());
        }
        assertEquals("重试次数应等于 retryCount", client.getRetryCount(), client.attemptBodies.size());
    }

    /** retryCount 被设成 0 时也必须真正发起一次写入，不能静默返回把数据丢掉 */
    @Test
    public void writesOnceWhenRetryCountIsZero() throws Exception {
        StubFeatureDBClient client = new StubFeatureDBClient(SUCCESS);
        client.setRetryCount(0);
        write(client, newData(InsertMode.FullRowWrite));
        assertEquals("retryCount=0 时仍应尝试一次", 1, client.attemptBodies.size());
    }

    /**
     * 关键回归点：__insert_mode__ 在构造请求前就被从 data 里移除了。
     * 如果请求体在重试时重新构造，write_mode 会退化成 Unknown，数据会被写错。
     */
    @Test
    public void keepsWriteModeIdenticalAcrossRetries() throws Exception {
        StubFeatureDBClient client = new StubFeatureDBClient(503, 503, SUCCESS);
        write(client, newData(InsertMode.PartialFieldWrite));

        assertEquals(3, client.attemptBodies.size());
        for (String requestBody : client.attemptBodies) {
            assertTrue("write_mode 丢失或被降级: " + requestBody,
                    requestBody.contains("\"write_mode\":\"PartialFieldWrite\""));
            assertFalse("write_mode 退化成 Unknown: " + requestBody,
                    requestBody.contains("Unknown"));
            assertFalse("内部标记不应发送给服务端: " + requestBody,
                    requestBody.contains("__insert_mode__"));
        }
        assertEquals("每次重试的请求体应完全一致",
                1, new java.util.HashSet<>(client.attemptBodies).size());
    }
}
