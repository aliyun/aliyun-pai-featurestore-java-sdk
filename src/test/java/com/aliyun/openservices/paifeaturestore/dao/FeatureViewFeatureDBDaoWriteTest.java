package com.aliyun.openservices.paifeaturestore.dao;

import com.aliyun.openservices.paifeaturestore.datasource.FeatureDBClient;
import com.aliyun.openservices.paifeaturestore.datasource.FeatureDBFactory;
import com.aliyun.openservices.paifeaturestore.datasource.HttpConfig;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 验证 FeatureViewFeatureDBDao 的写入批次上限与反压行为。
 * 用一个人为放慢的 FeatureDBClient 替换真实下游，不依赖网络。
 */
public class FeatureViewFeatureDBDaoWriteTest {

    // 与 FeatureViewFeatureDBDao 中的常量保持一致
    private static final int FLUSH_MAX_ROWS = 200;

    // 写出是同步的,所以链路上驻留的数据不会超过一批的量,这里留一倍余量
    private static final int EXPECTED_MAX_RESIDENT_ROWS = FLUSH_MAX_ROWS * 2;

    private static final int TOTAL_ROWS = 4000;

    private static final long SLOW_WRITE_MILLIS = 50;

    /** 记录收到的数据并模拟下游写入耗时的假 client */
    private static class RecordingFeatureDBClient extends FeatureDBClient {
        private final long delayMillis;
        private final AtomicInteger completedRows = new AtomicInteger();
        private final AtomicInteger maxBatchRows = new AtomicInteger();
        private final Map<Object, Integer> rowIdCounts = new ConcurrentHashMap<>();

        RecordingFeatureDBClient(long delayMillis) {
            super(new HttpConfig());
            this.delayMillis = delayMillis;
        }

        @Override
        public void writeFeatureDB(List<Map<String, Object>> data, String database, String schema, String table)
                throws Exception {
            maxBatchRows.accumulateAndGet(data.size(), Math::max);
            for (Map<String, Object> row : data) {
                rowIdCounts.merge(row.get("id"), 1, Integer::sum);
            }
            if (delayMillis > 0) {
                Thread.sleep(delayMillis);
            }
            completedRows.addAndGet(data.size());
        }
    }

    private static FeatureViewFeatureDBDao newDao(FeatureDBClient client, String name) {
        FeatureDBFactory.register(name, client);
        DaoConfig daoConfig = new DaoConfig();
        daoConfig.featureDBName = name;
        daoConfig.featureDBDatabase = "test_db";
        daoConfig.featureDBSchema = "test_schema";
        daoConfig.featureDBTable = "test_table";
        daoConfig.primaryKeyField = "id";
        return new FeatureViewFeatureDBDao(daoConfig);
    }

    /**
     * 按 Flink sink 的方式逐行写入，返回 {耗时毫秒, 观察到的最大堆积行数}。
     * 堆积行数 = 已提交行数 - 下游已完成行数，也就是整条链路上驻留的数据量。
     */
    private static long[] pushRows(FeatureViewFeatureDBDao dao, RecordingFeatureDBClient client, int totalRows) {
        long maxLag = 0;
        long start = System.currentTimeMillis();
        for (int i = 0; i < totalRows; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", i);
            List<Map<String, Object>> content = new ArrayList<>();
            content.add(row);
            dao.writeFeatures(content);
            maxLag = Math.max(maxLag, (i + 1) - client.completedRows.get());
        }
        return new long[]{System.currentTimeMillis() - start, maxLag};
    }

    private static void awaitDrain(RecordingFeatureDBClient client, int totalRows, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (client.completedRows.get() < totalRows && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
    }

    @Test(timeout = 120000)
    public void writeFeaturesBlocksWhenDownstreamIsSlow() throws Exception {
        // 基准:下游不耗时,写入方不该被拖慢,这一轮测的是纯开销
        RecordingFeatureDBClient fastClient = new RecordingFeatureDBClient(0);
        FeatureViewFeatureDBDao fastDao = newDao(fastClient, "fast-" + System.nanoTime());
        long[] fast = pushRows(fastDao, fastClient, TOTAL_ROWS);
        awaitDrain(fastClient, TOTAL_ROWS, 60000);

        // 每批写入耗时 50ms,写出是同步的,所以写入方必然被拖慢
        RecordingFeatureDBClient slowClient = new RecordingFeatureDBClient(SLOW_WRITE_MILLIS);
        FeatureViewFeatureDBDao slowDao = newDao(slowClient, "slow-" + System.nanoTime());
        long[] slow = pushRows(slowDao, slowClient, TOTAL_ROWS);
        awaitDrain(slowClient, TOTAL_ROWS, 60000);

        System.out.printf("fast: millis=%d maxLag=%d maxBatch=%d%n", fast[0], fast[1], fastClient.maxBatchRows.get());
        System.out.printf("slow: millis=%d maxLag=%d maxBatch=%d%n", slow[0], slow[1], slowClient.maxBatchRows.get());

        // 1) 单批行数不超过上限
        assertTrue("单批行数超过上限: " + slowClient.maxBatchRows.get(),
                slowClient.maxBatchRows.get() <= FLUSH_MAX_ROWS);
        assertTrue("单批行数超过上限: " + fastClient.maxBatchRows.get(),
                fastClient.maxBatchRows.get() <= FLUSH_MAX_ROWS);

        // 2) 堆积受控:没有反压时会一路堆到 TOTAL_ROWS
        assertTrue("写入方没有被阻塞,堆积达到 " + slow[1] + " 行,超过上限 " + EXPECTED_MAX_RESIDENT_ROWS,
                slow[1] <= EXPECTED_MAX_RESIDENT_ROWS);

        // 3) 下游慢时写入方确实被拖慢了,而不是立刻返回。
        // 下界按下游吞吐推算: TOTAL_ROWS / FLUSH_MAX_ROWS 批 * 50ms = 1000ms,取 300ms 留出余量
        assertTrue("下游变慢后写入方没有被阻塞,耗时仅 " + slow[0] + "ms", slow[0] >= 300);

        // 4) 分批与阻塞没有造成丢数据或重复写
        assertEquals("写入行数不一致", TOTAL_ROWS, slowClient.completedRows.get());
        assertEquals("行数不一致,可能有丢失或重复", TOTAL_ROWS, slowClient.rowIdCounts.size());
        for (Map.Entry<Object, Integer> entry : slowClient.rowIdCounts.entrySet()) {
            assertEquals("行 " + entry.getKey() + " 被写入了多次", Integer.valueOf(1), entry.getValue());
        }
    }
}
