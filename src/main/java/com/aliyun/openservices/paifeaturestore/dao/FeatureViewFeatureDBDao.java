package com.aliyun.openservices.paifeaturestore.dao;

import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.aliyun.openservices.paifeaturestore.datasource.*;
import com.aliyun.openservices.paifeaturestore.domain.FeatureResult;
import com.aliyun.openservices.paifeaturestore.domain.FeatureStoreResult;
import com.aliyun.openservices.paifeaturestore.domain.FeatureView;
import com.aliyun.openservices.paifeaturestore.model.FeatureViewSeqConfig;
import com.aliyun.openservices.paifeaturestore.model.SeqConfig;
import com.aliyun.openservices.paifeaturestore.model.SequenceInfo;
import com.aliyun.tea.utils.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bouncycastle.util.Strings;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

    private List<String> fields;

    private final List<Map<String, Object>> writeData = new ArrayList<>();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final ExecutorService executor = Executors.newFixedThreadPool(4);
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
        this.fields = daoConfig.fields;
        this.startAsyncWrite();
    }

    @Override
    public FeatureResult getFeatures(String[] keys, String[] selectFields) {
        Set<String> selectFieldSet = new HashSet<>(Arrays.asList(selectFields));
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
                if(content!=null) {
                    RecordBlock recordBlock = RecordBlock.getRootAsRecordBlock(ByteBuffer.wrap(content));
                    for (int i = 0; i < recordBlock.valuesLength(); i++) {
                        UInt8ValueColumn valueColumn = new UInt8ValueColumn();
                        recordBlock.values(valueColumn, i);
                        if (valueColumn.valueLength() < 2) {
                            continue;
                        }
                        ByteBuffer byteBuffer = valueColumn.valueAsByteBuffer();
                        byteBuffer = byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
                        byte protoFlag = byteBuffer.get();
                        byte protoVersion = byteBuffer.get();
                        if (protoFlag != 'F' || protoVersion != '1') {
                            String errorMsg = String.format("invalid proto version, %d, %d", protoFlag, protoVersion);
                            log.error(errorMsg);
                            throw new RuntimeException(errorMsg);
                        }
                        Map<String, Object> featureMap = new HashMap<>(selectFields.length);

                        for (String featureName : this.fields) {
                            if (featureName.equals(this.primaryKeyField)) {
                                continue;
                            }
                            byte isNull = byteBuffer.get();
                            if (1 == isNull) {
                                if (selectFieldSet.contains(featureName)) {
                                    featureMap.put(featureName, null);
                                }
                                continue;
                            }
                            switch (this.fieldTypeMap.get(featureName)) {
                                case FS_INT32:
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, byteBuffer.getInt());
                                    }
                                    break;
                                case FS_INT64:
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, byteBuffer.getLong());
                                    }
                                    break;
                                case FS_DOUBLE:
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, byteBuffer.getDouble());
                                    }
                                    break;
                                case FS_BOOLEAN:
                                    byte boolValue = byteBuffer.get();
                                    if (boolValue == 0) {
                                        if (selectFieldSet.contains(featureName)) {
                                            featureMap.put(featureName, false);
                                        }
                                    } else {
                                        if (selectFieldSet.contains(featureName)) {
                                            featureMap.put(featureName, true);
                                        }
                                    }
                                    break;
                                case FS_FLOAT:
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, byteBuffer.getFloat());
                                    }
                                    break;
                                default:
                                    int len = byteBuffer.getInt();
                                    byte[] bytes = new byte[len];
                                    byteBuffer.get(bytes, 0, len);
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, new String(bytes));
                                    }
                                    break;
                            }

                        }

                        featureMap.put(this.primaryKeyField, group.get(i));
                        featuresList.add(featureMap);
                    }
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
        FeatureStoreResult featureStoreResult = new FeatureStoreResult();
        String[] selectFields=null;
        if (!StringUtils.isEmpty(featureViewSeqConfig.getPlayTimeField())) {
            selectFields=new String[]{featureViewSeqConfig.getItemIdField(),featureViewSeqConfig.getEventField(),featureViewSeqConfig.getPlayTimeField(),featureViewSeqConfig.getTimestampField()};
        } else {
            selectFields=new String[]{featureViewSeqConfig.getItemIdField(),featureViewSeqConfig.getEventField(),featureViewSeqConfig.getTimestampField()};
        }
        long currentime = System.currentTimeMillis()/1000;
        HashMap<String, Double> playtimefilter = new HashMap<>();
        if (!StringUtils.isEmpty(featureViewSeqConfig.getPlayTimeFilter())) {
            for (String event: Strings.split(featureViewSeqConfig.getPlayTimeFilter(), ';')) {
                String[] s = Strings.split(event, ':');
                if (s.length == 2) {//key有值
                    playtimefilter.put(s[0], Double.valueOf(s[1]));
                }
            }
        }
        String[] events = new String[featureViewSeqConfig.getSeqConfigs().length];
        for (int i = 0;i< events.length;i++){
            events[i] = featureViewSeqConfig.getSeqConfigs()[i].getSeqEvent();
        }
        Set<String> featureFieldList = new HashSet<>();
        List<Map<String, Object>> featureDataList = new ArrayList<>();
        for (String key:keys){
            HashMap<String, String> keyEventsDatasOnline = new HashMap<>();
            for (String event:events){
                List<SequenceInfo> onlineSequence = fetchData(key, playtimefilter, featureViewSeqConfig, event, userIdField,currentime, true);
                List<SequenceInfo> offlineSequence = new ArrayList<>();
                onlineSequence = MergeOnOfflineSeq(onlineSequence, offlineSequence, featureViewSeqConfig, event);
                Map<String, String> resultData = disposeDB(onlineSequence, selectFields, featureViewSeqConfig, event, currentime);
                if (onlineSequence.size()>0) {
                    keyEventsDatasOnline.putAll(resultData);
                }

            }

            if (keyEventsDatasOnline.size()>0) {
                keyEventsDatasOnline.put(this.primaryKeyField,key);
            }

            if (!keyEventsDatasOnline.isEmpty()) {
                featureFieldList.addAll(keyEventsDatasOnline.keySet());

                boolean found = false;
                for (Map<String, Object> features : featureDataList) {
                    if (features.containsKey(keyEventsDatasOnline.get(this.primaryKeyField))) {
                        for (Map.Entry<String, String> entry : keyEventsDatasOnline.entrySet()) {
                            features.put(entry.getKey(), entry.getKey());
                        }
                        found = true;
                        break;
                    }

                }

                if (!found) {
                    Map<String, Object> featureData = new HashMap<>();
                    for (Map.Entry<String, String> entry : keyEventsDatasOnline.entrySet()) {
                        featureData.put(entry.getKey(), entry.getValue());
                    }
                    featureDataList.add(featureData);
                }
            }
        }
        String[] fields = new String[featureFieldList.size()];
        int f=0;
        for (String field:featureFieldList) {
            fields[f++]=field;
        }

        Map<String, FSType> featureFieldTypeMap = new HashMap<>();
        for (String featureName : featureFieldList) {
            featureFieldTypeMap.put(featureName, FSType.FS_STRING);
        }
        featureStoreResult.setFeatureFields(featureFieldList.toArray(new String[0]));
        featureStoreResult.setFeatureFieldTypeMap(featureFieldTypeMap);
        featureStoreResult.setFeatureFields(fields);
        featureStoreResult.setFeatureDataList(featureDataList);
        return featureStoreResult;
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

    @Override
    public void writeFlush() {
        lock.lock();
        try {
            if (writeData.size() > 0) {
                try {
                    doWriteFeatures();
                    this.executor.shutdown();
                    try {
                        if (!this.executor.awaitTermination(60, TimeUnit.SECONDS)) {
                            this.executor.shutdownNow(); // 取消正在执行的任务
                        }
                    } catch (InterruptedException e) {
                        this.executor.shutdownNow();
                    }

                } catch (Exception e) {
                    log.error(String.format("request featuredb error:%s", e.getMessage()));
                }
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
    public List<SequenceInfo> fetchData(String key,  HashMap<String, Double> playtimefilter,
                                        FeatureViewSeqConfig config, String event, String userIdFields, Long currentime, boolean useOnlineTable)  {
        List<SequenceInfo> sequenceInfos = new ArrayList<>();
        List<String> pks = new ArrayList<>();
        for (String e: event.split("\\|") ){
            pks.add(String.format("%s\u001D%s", key, e));
        }
        try {
            byte[] content = this.featureDBClient.kkvRequestFeatureDB(pks, this.database, this.schema, this.table, config.getSeqLenOnline());
            if (content!=null){
                KKVRecordBlock kkvRecordBlock = KKVRecordBlock.getRootAsKKVRecordBlock(ByteBuffer.wrap(content));
                for (int i = 0; i< kkvRecordBlock.valuesLength(); i++){
                    KKVData kkvData = new KKVData();
                    kkvRecordBlock.values(kkvData, i);
                    String pk = kkvData.pk();
                    String[] userIdEvent = pk.split("\u001D");
                    if (userIdEvent.length != 2) {
                        continue;
                    }
                    String itemId = "";
                    if (config.getDeduplicationMethodNum() == 1){
                        itemId = kkvData.sk();
                    } else if (config.getDeduplicationMethodNum() == 2){
                        String sk = kkvData.sk();
                        String[] itemIdTimestamp = sk.split("\u001D");
                        if (itemIdTimestamp.length != 2){
                            continue;
                        }
                        itemId = itemIdTimestamp[0];
                    } else {
                        continue;
                    }
                    SequenceInfo sequenceInfo = new SequenceInfo();
                    sequenceInfo.setEventField(userIdEvent[1]);
                    sequenceInfo.setItemIdField(Long.valueOf(itemId));
                    sequenceInfo.setPlayTimeField(kkvData.playTime());
                    sequenceInfo.setTimestampField(kkvData.eventTimestamp());
                    if (Objects.equals(sequenceInfo.getEventField(), "") || sequenceInfo.getItemIdField() == 0){
                        continue;

                    }
                    if(playtimefilter.containsKey(sequenceInfo.getEventField())){
                        double t = playtimefilter.get(sequenceInfo.getEventField());
                        if (sequenceInfo.getPlayTimeField() <= t){
                            continue;
                        }
                    }
                    sequenceInfos.add(sequenceInfo);
                }
            }


        } catch (Exception e) {
            log.error(String.format("request featuredb error:%s", e.getMessage()));
            throw new RuntimeException(e);
        }
        return sequenceInfos;
    }

    public List<SequenceInfo> MergeOnOfflineSeq(List<SequenceInfo> offlineSequence, List<SequenceInfo> onlineSequence,FeatureViewSeqConfig config,String event){

        if (offlineSequence.isEmpty()) {
            return onlineSequence;
        } else if(onlineSequence.isEmpty()) {
            return offlineSequence;
        } else {
            int index=0;
            for (;index<onlineSequence.size();) {
                if (Long.valueOf(onlineSequence.get(index).getTimestampField()) < Long.valueOf(offlineSequence.get(0).getTimestampField())) {
                    break;
                }
                index++;
            }
            onlineSequence=onlineSequence.subList(0,index);
            onlineSequence.addAll(offlineSequence);
            if (onlineSequence.size() > config.getSeqLenOnline()) {
                onlineSequence.subList(0,config.getSeqLenOnline());
            }

        }
        return onlineSequence;
    }
    public Map<String,String> disposeDB(List<SequenceInfo> sequenceInfos,String[] selectFields,FeatureViewSeqConfig config,String event,Long currentime) {
        HashMap<String, String> sequenceFeatures = new HashMap<>();
        for (SequenceInfo sequenceInfo:sequenceInfos) {
            String qz="";
            for (SeqConfig s:config.getSeqConfigs()) {
                if (s.getSeqEvent().equals(event)) {
                    qz=s.getOnlineSeqName();
                    break;
                }
            }
            for (String name : selectFields) {
                String newname = qz + "__" + name;

                if (name.equals(config.getItemIdField())) {
                    if (sequenceFeatures.containsKey(newname)) {
                        sequenceFeatures.put(newname, sequenceFeatures.get(newname) + ";" + sequenceInfo.getItemIdField());
                    } else {
                        sequenceFeatures.put(newname, ""+sequenceInfo.getItemIdField());
                    }
                    if (sequenceFeatures.containsKey(qz)) {
                        sequenceFeatures.put(qz, sequenceFeatures.get(qz) + ";" + sequenceInfo.getItemIdField());
                    } else {
                        sequenceFeatures.put(qz, ""+sequenceInfo.getItemIdField());
                    }
                } else if (name.equals(config.getTimestampField())) {
                    if (sequenceFeatures.containsKey(newname)) {
                        sequenceFeatures.put(newname, sequenceFeatures.get(newname) + ";" + sequenceInfo.getTimestampField());
                    } else {
                        sequenceFeatures.put(newname, ""+sequenceInfo.getTimestampField());
                    }
                } else if (name.equals(config.getEventField())) {
                    if (sequenceFeatures.containsKey(newname)) {
                        sequenceFeatures.put(newname, sequenceFeatures.get(newname) + ";" + sequenceInfo.getEventField());
                    } else {
                        sequenceFeatures.put(newname, sequenceInfo.getEventField());
                    }
                } else if (name.equals(config.getPlayTimeField())) {
                    if (sequenceFeatures.containsKey(newname)) {
                        sequenceFeatures.put(newname, sequenceFeatures.get(newname) + ";" + sequenceInfo.getPlayTimeField());
                    } else {
                        sequenceFeatures.put(newname, ""+sequenceInfo.getPlayTimeField());
                    }

                }
            }
            String tsfields = qz + "__ts";//Timestamp from the current time
            long eventTime = 0;
            if (!StringUtils.isEmpty(sequenceInfo.getTimestampField())) {
                eventTime =Long.valueOf(sequenceInfo.getTimestampField());
            }
            if (sequenceFeatures.containsKey(tsfields)) {
                sequenceFeatures.put(tsfields, sequenceFeatures.get(tsfields) + ";" + (currentime - eventTime));
            } else {
                sequenceFeatures.put(tsfields, String.valueOf((currentime - eventTime)));


            }
        }

        return sequenceFeatures;
    }
}
