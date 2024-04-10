package com.aliyun.openservices.paifeaturestore.dao;

import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.aliyun.openservices.paifeaturestore.datasource.FeatureDBClient;
import com.aliyun.openservices.paifeaturestore.datasource.FeatureDBFactory;
import com.aliyun.openservices.paifeaturestore.datasource.RecordBlock;
import com.aliyun.openservices.paifeaturestore.datasource.UInt8ValueColumn;
import com.aliyun.openservices.paifeaturestore.domain.FeatureResult;
import com.aliyun.openservices.paifeaturestore.domain.FeatureStoreResult;
import com.aliyun.openservices.paifeaturestore.model.FeatureViewSeqConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class FeatureViewFeatureDBDao implements FeatureViewDao {
    private static Log log = LogFactory.getLog(FeatureViewFeatureDBDao.class);//日志工厂
    private FeatureDBClient featureDBClient;

    private String database;

    private String schema;

    private String table;

    private String primaryKeyField;

    public Map<String, FSType> fieldTypeMap;

    private final List<Map<String, Object>> writeData = new ArrayList<>();

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition condition = lock.newCondition();
    private final ExecutorService executor = Executors.newFixedThreadPool(8);
    public FeatureViewFeatureDBDao(DaoConfig daoConfig) {
        this.database = daoConfig.featureDBDatabase;
        this.schema = daoConfig.featureDBSchema;
        this.table = daoConfig.featureDBTable;

        FeatureDBClient client = FeatureDBFactory.get(daoConfig.featureDBName);
        if (null == client) {
            throw  new RuntimeException(String.format("featuredbclient:%s not found", daoConfig.featureDBName));
        }

        this.featureDBClient = client;
        this.fieldTypeMap = daoConfig.fieldTypeMap;
        this.primaryKeyField = daoConfig.primaryKeyField;
        this.startAsyncWrite();
    }

    @Override
    public FeatureResult getFeatures(String[] keys, String[] selectFields) {
        List<String> keyList = Arrays.asList(keys);
        final int GROUP_SIZE = 200;
        List<List<String>> groups = new ArrayList<>();
        for (int i = 0; i < keyList.size(); i+= GROUP_SIZE) {
            int end = i + GROUP_SIZE;
            if (end > keyList.size()) {
                end = keyList.size();
            }

            groups.add(keyList.subList(i, end));
        }

        List<CompletableFuture<FeatureStoreResult>> futures =  groups.stream().map(group-> CompletableFuture.supplyAsync(()->{

            FeatureStoreResult featureResult = new FeatureStoreResult();
            List<Map<String, Object>> featuresList = new ArrayList<>(group.size());
            try {
                byte[] content = this.featureDBClient.requestFeatureDB(group, this.database, this.schema, this.table);

                RecordBlock recordBlock = RecordBlock.getRootAsRecordBlock(ByteBuffer.wrap(content));
                for(int i= 0; i < recordBlock.valuesLength(); i++) {
                    UInt8ValueColumn valueColumn = new UInt8ValueColumn();
                    recordBlock.values(valueColumn, i);
                    if (valueColumn.valueLength() < 2 ) {
                        continue;
                    }
                    ByteBuffer byteBuffer =  valueColumn.valueAsByteBuffer();
                    byteBuffer =  byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
                    byte protoFlag =  byteBuffer.get();
                    byte protoVersion =  byteBuffer.get();
                    if (protoFlag != 'F' || protoVersion != '1') {
                        String errorMsg =String.format("invalid proto version, %d, %d", protoFlag, protoVersion);
                        log.error(errorMsg);
                        throw  new RuntimeException(errorMsg);
                    }
                    Map<String, Object> featureMap = new HashMap<>(selectFields.length);

                    for (String featureName : selectFields) {
                        if (featureName.equals(this.primaryKeyField)) {
                            continue;
                        }
                        byte isNull = byteBuffer.get();
                        if (1 == isNull) {
                            featureMap.put(featureName, null);
                            continue;
                        }
                        switch (this.fieldTypeMap.get(featureName)) {
                            case FS_INT32:
                                featureMap.put(featureName, byteBuffer.getInt());
                                break;
                            case FS_INT64:
                                featureMap.put(featureName, byteBuffer.getLong());
                                break;
                            case FS_DOUBLE:
                                featureMap.put(featureName, byteBuffer.getDouble());
                                break;
                            case FS_BOOLEAN:
                                byte boolValue = byteBuffer.get();
                                if (boolValue == 0) {
                                    featureMap.put(featureName, false);
                                } else {
                                    featureMap.put(featureName, true);
                                }
                                break;
                            case FS_FLOAT:
                                featureMap.put(featureName, byteBuffer.getFloat());
                                break;
                            default:
                                int len = byteBuffer.getInt();
                                byte[] bytes = new byte[len];
                                byteBuffer.get(bytes, 0, len);
                                featureMap.put(featureName, new String(bytes));
                                break;
                        }

                    }

                    featureMap.put(this.primaryKeyField, group.get(i));
                    featuresList.add(featureMap);
                }

                featureResult.setFeatureDataList(featuresList);
            } catch (Exception e) {
                log.error(String.format("request featuredb error:%s", e.getMessage()));
                throw new RuntimeException(e);
            }
            return featureResult;
        })).collect(Collectors.toList());

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        CompletableFuture<List<FeatureStoreResult>> allFutureResults = allFutures.thenApply(v ->
                futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList())
        );

        //List<FeatureResult> featureResultList = completableFutureStream.map(CompletableFuture::join).collect(Collectors.toList());
        List<FeatureStoreResult> featureResultList = null;
        try {
            featureResultList = allFutureResults.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        FeatureStoreResult featureResult = new FeatureStoreResult();
        featureResult.setFeatureFields(selectFields);
        featureResult.setFeatureFieldTypeMap(this.fieldTypeMap);

        List<Map<String, Object>> featureDataList = new ArrayList<>(keys.length);
        for (FeatureResult result : featureResultList) {
            if (null != result.getFeatureData()) {
               featureDataList.addAll(result.getFeatureData());
            }
        }


        featureResult.setFeatureDataList(featureDataList);
        return featureResult;
    }

    @Override
    public FeatureResult getSequenceFeatures(String[] keys, String userIdField, FeatureViewSeqConfig featureViewSeqConfig) {
        return null;
    }

    @Override
    public void writeFeatures(List<Map<String, Object>> data) {
        lock.lock();
        try {
            writeData.addAll(data);
            if (writeData.size() >= 20) {
                condition.signal();
            }
        } finally {
            lock.unlock();
        }

    }

    private void startAsyncWrite() {
        new Thread(() -> {
            while (true) {
                lock.lock();
                try {
                    condition.await(50, java.util.concurrent.TimeUnit.MILLISECONDS);
                    if (!writeData.isEmpty()) {
                        doWriteFeatures();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
            }
        }).start();
    }

    private void doWriteFeatures() {
        List<Map<String, Object>> tempList = new ArrayList<>(writeData);
        writeData.clear();

        // 异步处理 tempList
        this.executor.submit(()->{
            try {
                this.featureDBClient.writeFeatureDB(tempList, this.database, this.schema, this.table);
            } catch (Exception e) {
                log.error(String.format("request featuredb error:%s", e.getMessage()));
            }
        });
    }
}
