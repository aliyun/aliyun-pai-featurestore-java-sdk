package com.aliyun.openservices.paifeaturestore.dao;

import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.aliyun.openservices.paifeaturestore.datasource.*;
import com.aliyun.openservices.paifeaturestore.domain.FeatureResult;
import com.aliyun.openservices.paifeaturestore.domain.FeatureStoreResult;
import com.aliyun.openservices.paifeaturestore.model.FeatureViewSeqConfig;
import com.aliyun.openservices.paifeaturestore.model.SeqConfig;
import com.aliyun.openservices.paifeaturestore.model.SequenceInfo;
import com.aliyun.tea.utils.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bouncycastle.util.Strings;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
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
            throw new RuntimeException(String.format("featuredbclient:%s not found", daoConfig.featureDBName));
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
        for (int i = 0; i < keyList.size(); i += GROUP_SIZE) {
            int end = i + GROUP_SIZE;
            if (end > keyList.size()) {
                end = keyList.size();
            }

            groups.add(keyList.subList(i, end));
        }

        List<CompletableFuture<FeatureStoreResult>> futures = groups.stream().map(group -> CompletableFuture.supplyAsync(() -> {

            FeatureStoreResult featureResult = new FeatureStoreResult();
            List<Map<String, Object>> featuresList = new ArrayList<>(group.size());
            try {
                byte[] content = this.featureDBClient.requestFeatureDB(group, this.database, this.schema, this.table);
                if (content != null) {
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
                                    Integer intValue = byteBuffer.getInt();
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, intValue);
                                    }
//                                    if (selectFieldSet.contains(featureName)) {
//                                        featureMap.put(featureName, byteBuffer.getInt());
//                                    }
                                    break;
                                case FS_INT64:
                                    Long longValue = byteBuffer.getLong();
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, longValue);
                                    }
//                                    if (selectFieldSet.contains(featureName)) {
//                                        featureMap.put(featureName, byteBuffer.getLong());
//                                    }
                                    break;
                                case FS_DOUBLE:
                                    Double doubleValue = byteBuffer.getDouble();
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, doubleValue);
                                    }
//                                    if (selectFieldSet.contains(featureName)) {
//                                        featureMap.put(featureName, byteBuffer.getDouble());
//                                    }
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
                                    Float floatValue = byteBuffer.getFloat();
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, floatValue);
                                    }
//                                    if (selectFieldSet.contains(featureName)) {
//                                        featureMap.put(featureName, byteBuffer.getFloat());
//                                    }
                                    break;
                                case FS_ARRAY_INT32:

                                    int lenArrayInt32 = byteBuffer.getInt();
                                    List<Integer> integerList = new ArrayList<>(lenArrayInt32);
                                    for (int j = 0; j < lenArrayInt32; j++) {
                                        integerList.add(byteBuffer.getInt());
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, integerList);

                                    }
                                    break;
                                case FS_ARRAY_INT64:

                                    int lenArrayInt64 = byteBuffer.getInt();
                                    List<Long> longList = new ArrayList<>(lenArrayInt64);
                                    for (int j = 0; j < lenArrayInt64; j++) {
                                        longList.add(byteBuffer.getLong());
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, longList);
                                    }
                                    break;
                                case FS_ARRAY_FLOAT:

                                    int lenArrayFloat = byteBuffer.getInt();
                                    List<Float> floatList = new ArrayList<>(lenArrayFloat);
                                    for (int j = 0; j < lenArrayFloat; j++) {
                                        floatList.add(byteBuffer.getFloat());
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, floatList);
                                    }
                                    break;
                                case FS_ARRAY_DOUBLE:

                                    int lenArrayDouble = byteBuffer.getInt();
                                    List<Double> doubleList = new ArrayList<>(lenArrayDouble);
                                    for (int j = 0; j < lenArrayDouble; j++) {
                                        doubleList.add(byteBuffer.getDouble());
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, doubleList);
                                    }
                                    break;
                                case FS_ARRAY_STRING:

                                    int lenArrayString = byteBuffer.getInt();
                                    String[] arrayStringValue = decodeStringArray(byteBuffer, lenArrayString);
                                    List<String> stringList = Arrays.asList(arrayStringValue);
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, stringList);
                                    }
                                    break;
                                case FS_ARRAY_ARRAY_FLOAT:

                                    int outerLength = byteBuffer.getInt();
                                    List<List<Float>> arrayOfArrayFloatValue = new ArrayList<>(outerLength);

                                    if (outerLength > 0) {
                                        int totalElements = byteBuffer.getInt();

                                        if (totalElements == 0) {
                                            for (int outerIdx = 0; outerIdx < outerLength; outerIdx++) {
                                                arrayOfArrayFloatValue.add(new ArrayList<>());
                                            }
                                        } else {
                                            int[] innerArrayLens = new int[outerLength];
                                            for (int j = 0; j < outerLength; j++) {
                                                innerArrayLens[j] = byteBuffer.getInt();
                                            }

                                            List<Float> innerValidElements = new ArrayList<>(totalElements);
                                            for (int j = 0; j < totalElements; j++) {
                                                innerValidElements.add(byteBuffer.getFloat());
                                            }

                                            int innerIndex = 0;
                                            for (int outerIdx = 0; outerIdx < outerLength; outerIdx++) {
                                                int innerLength = innerArrayLens[outerIdx];
                                                List<Float> innerArray = new ArrayList<>();
                                                for (int j = 0; j < innerLength; j++) {
                                                    innerArray.add(innerValidElements.get(innerIndex++));
                                                }
                                                arrayOfArrayFloatValue.add(innerArray);
                                            }
                                        }
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, arrayOfArrayFloatValue);
                                    }
                                    break;
                                case FS_MAP_INT32_INT32:

                                    int lenMapInt32Int32 = byteBuffer.getInt();
                                    Map<Integer, Integer> mapInt32Int32Value = new HashMap<>(lenMapInt32Int32);

                                    if (lenMapInt32Int32 > 0) {
                                        int[] keyArray = new int[lenMapInt32Int32];

                                        for (int j = 0; j < lenMapInt32Int32; j++) {
                                            keyArray[j] = byteBuffer.getInt();
                                        }

                                        int[] values = new int[lenMapInt32Int32];
                                        for (int j = 0; j < lenMapInt32Int32; j++) {
                                            values[j] = byteBuffer.getInt();
                                        }

                                        for (int idx = 0; idx < lenMapInt32Int32; idx++) {
                                            mapInt32Int32Value.put(keyArray[idx], values[idx]);
                                        }
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, mapInt32Int32Value);
                                    }
                                    break;

                                case FS_MAP_INT32_INT64:

                                    int lenMapInt32Int64 = byteBuffer.getInt();
                                    Map<Integer, Long> mapInt32Int64Value = new HashMap<>(lenMapInt32Int64);
                                    if (lenMapInt32Int64 > 0) {
                                        int[] keyArray = new int[lenMapInt32Int64];

                                        for (int j = 0; j < lenMapInt32Int64; j++) {
                                            keyArray[j] = byteBuffer.getInt();
                                        }

                                        long[] values = new long[lenMapInt32Int64];
                                        for (int j = 0; j < lenMapInt32Int64; j++) {
                                            values[j] = byteBuffer.getLong();
                                        }

                                        for (int idx = 0; idx < lenMapInt32Int64; idx++) {
                                            mapInt32Int64Value.put(keyArray[idx], values[idx]);
                                        }
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, mapInt32Int64Value);
                                    }
                                    break;
                                case FS_MAP_INT32_FLOAT:

                                    int lenMapInt32Float = byteBuffer.getInt();
                                    Map<Integer, Float> mapInt32FloatValue = new HashMap<>(lenMapInt32Float);
                                    if (lenMapInt32Float > 0) {
                                        int[] keyArray = new int[lenMapInt32Float];

                                        for (int j = 0; j < lenMapInt32Float; j++) {
                                            keyArray[j] = byteBuffer.getInt();
                                        }

                                        float[] values = new float[lenMapInt32Float];
                                        for (int j = 0; j < lenMapInt32Float; j++) {
                                            values[j] = byteBuffer.getFloat();
                                        }

                                        for (int idx = 0; idx < lenMapInt32Float; idx++) {
                                            mapInt32FloatValue.put(keyArray[idx], values[idx]);
                                        }
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, mapInt32FloatValue);
                                    }
                                    break;
                                case FS_MAP_INT32_DOUBLE:

                                    int lenMapInt32Double = byteBuffer.getInt();
                                    Map<Integer, Double> mapInt32DoubleValue = new HashMap<>(lenMapInt32Double);
                                    if (lenMapInt32Double > 0) {
                                        int[] keyArray = new int[lenMapInt32Double];

                                        for (int j = 0; j < lenMapInt32Double; j++) {
                                            keyArray[j] = byteBuffer.getInt();
                                        }
                                        double[] values = new double[lenMapInt32Double];
                                        for (int j = 0; j < lenMapInt32Double; j++) {
                                            values[j] = byteBuffer.getDouble();
                                        }
                                        for (int idx = 0; idx < lenMapInt32Double; idx++) {
                                            mapInt32DoubleValue.put(keyArray[idx], values[idx]);
                                        }
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, mapInt32DoubleValue);
                                    }
                                    break;
                                case FS_MAP_INT32_STRING:

                                    int lenMapInt32String = byteBuffer.getInt();
                                    Map<Integer, String> mapInt32StringValue = new HashMap<>(lenMapInt32String);

                                    if (lenMapInt32String > 0) {
                                        int[] keyArray = new int[lenMapInt32String];
                                        for (int j = 0; j < lenMapInt32String; j++) {
                                            keyArray[j] = byteBuffer.getInt();
                                        }


                                        String[] values = decodeStringArray(byteBuffer, lenMapInt32String);

                                        for (int idx = 0; idx < lenMapInt32String; idx++) {
                                            mapInt32StringValue.put(keyArray[idx], values[idx]);
                                        }
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, mapInt32StringValue);
                                    }
                                    break;
                                case FS_MAP_INT64_INT32:

                                    int lenMapInt64Int32 = byteBuffer.getInt();
                                    Map<Long, Integer> mapInt64Int32Value = new HashMap<>(lenMapInt64Int32);
                                    if (lenMapInt64Int32 > 0) {
                                        long[] keyArray = new long[lenMapInt64Int32];

                                        for (int j = 0; j < lenMapInt64Int32; j++) {
                                            keyArray[j] = byteBuffer.getLong();
                                        }

                                        int[] values = new int[lenMapInt64Int32];
                                        for (int j = 0; j < lenMapInt64Int32; j++) {
                                            values[j] = byteBuffer.getInt();
                                        }

                                        for (int idx = 0; idx < lenMapInt64Int32; idx++) {
                                            mapInt64Int32Value.put(keyArray[idx], values[idx]);
                                        }
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, mapInt64Int32Value);
                                    }
                                    break;
                                case FS_MAP_INT64_INT64:

                                    int lenMapInt64Int64 = byteBuffer.getInt();
                                    Map<Long, Long> mapInt64Int64Value = new HashMap<>(lenMapInt64Int64);
                                    if (lenMapInt64Int64 > 0) {
                                        long[] keyArray = new long[lenMapInt64Int64];

                                        for (int j = 0; j < lenMapInt64Int64; j++) {
                                            keyArray[j] = byteBuffer.getLong();
                                        }

                                        long[] values = new long[lenMapInt64Int64];
                                        for (int j = 0; j < lenMapInt64Int64; j++) {
                                            values[j] = byteBuffer.getLong();
                                        }
                                        for (int idx = 0; idx < lenMapInt64Int64; idx++) {
                                            mapInt64Int64Value.put(keyArray[idx], values[idx]);
                                        }
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, mapInt64Int64Value);
                                    }
                                    break;
                                case FS_MAP_INT64_FLOAT:

                                    int lenMapInt64Float = byteBuffer.getInt();
                                    Map<Long, Float> mapInt64FloatValue = new HashMap<>(lenMapInt64Float);

                                    if (lenMapInt64Float > 0) {
                                        long[] keyArray = new long[lenMapInt64Float];
                                        for (int j = 0; j < lenMapInt64Float; j++) {
                                            keyArray[j] = byteBuffer.getLong();
                                        }


                                        float[] values = new float[lenMapInt64Float];
                                        for (int j = 0; j < lenMapInt64Float; j++) {
                                            values[j] = byteBuffer.getFloat();
                                        }
                                        for (int idx = 0; idx < lenMapInt64Float; idx++) {
                                            mapInt64FloatValue.put(keyArray[idx], values[idx]);
                                        }
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, mapInt64FloatValue);
                                    }
                                    break;
                                case FS_MAP_INT64_DOUBLE:

                                    int lenMapInt64Double = byteBuffer.getInt();
                                    Map<Long, Double> mapInt64DoubleValue = new HashMap<>(lenMapInt64Double);

                                    if (lenMapInt64Double > 0) {
                                        long[] keyArray = new long[lenMapInt64Double];
                                        for (int j = 0; j < lenMapInt64Double; j++) {
                                            keyArray[j] = byteBuffer.getLong();
                                        }


                                        double[] values = new double[lenMapInt64Double];
                                        for (int j = 0; j < lenMapInt64Double; j++) {
                                            values[j] = byteBuffer.getDouble();
                                        }
                                        for (int idx = 0; idx < lenMapInt64Double; idx++) {
                                            mapInt64DoubleValue.put(keyArray[idx], values[idx]);
                                        }
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, mapInt64DoubleValue);
                                    }
                                    break;
                                case FS_MAP_INT64_STRING:

                                    int lenMapInt64String = byteBuffer.getInt();
                                    Map<Long, String> mapInt64StringValue = new HashMap<>(lenMapInt64String);

                                    if (lenMapInt64String > 0) {
                                        long[] keyArray = new long[lenMapInt64String];
                                        for (int j = 0; j < lenMapInt64String; j++) {
                                            keyArray[j] = byteBuffer.getLong();
                                        }


                                        String[] values = decodeStringArray(byteBuffer, lenMapInt64String);

                                        for (int idx = 0; idx < lenMapInt64String; idx++) {
                                            mapInt64StringValue.put(keyArray[idx], values[idx]);
                                        }
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, mapInt64StringValue);
                                    }
                                    break;
                                case FS_MAP_STRING_INT32:

                                    int lenMapStringInt32 = byteBuffer.getInt();
                                    Map<String, Integer> mapStringInt32Value = new HashMap<>(lenMapStringInt32);

                                    if (lenMapStringInt32 > 0) {
                                        String[] keyArray = decodeStringArray(byteBuffer, lenMapStringInt32);


                                        int[] values = new int[lenMapStringInt32];
                                        for (int j = 0; j < lenMapStringInt32; j++) {
                                            values[j] = byteBuffer.getInt();
                                        }
                                        for (int idx = 0; idx < lenMapStringInt32; idx++) {
                                            mapStringInt32Value.put(keyArray[idx], values[idx]);
                                        }
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, mapStringInt32Value);
                                    }
                                    break;
                                case FS_MAP_STRING_INT64:

                                    int lenMapStringInt64 = byteBuffer.getInt();
                                    Map<String, Long> mapStringInt64Value = new HashMap<>(lenMapStringInt64);
                                    if (lenMapStringInt64 > 0) {
                                        String[] keyArray = decodeStringArray(byteBuffer, lenMapStringInt64);


                                        long[] values = new long[lenMapStringInt64];
                                        for (int j = 0; j < lenMapStringInt64; j++) {
                                            values[j] = byteBuffer.getLong();
                                        }
                                        for (int idx = 0; idx < lenMapStringInt64; idx++) {
                                            mapStringInt64Value.put(keyArray[idx], values[idx]);
                                        }
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, mapStringInt64Value);
                                    }
                                    break;
                                case FS_MAP_STRING_FLOAT:

                                    int lenMapStringFloat = byteBuffer.getInt();
                                    Map<String, Float> mapStringFloatValue = new HashMap<>(lenMapStringFloat);

                                    if (lenMapStringFloat > 0) {
                                        String[] keyArray = decodeStringArray(byteBuffer, lenMapStringFloat);


                                        float[] values = new float[lenMapStringFloat];
                                        for (int j = 0; j < lenMapStringFloat; j++) {
                                            values[j] = byteBuffer.getFloat();
                                        }
                                        for (int idx = 0; idx < lenMapStringFloat; idx++) {
                                            mapStringFloatValue.put(keyArray[idx], values[idx]);
                                        }
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, mapStringFloatValue);
                                    }
                                    break;
                                case FS_MAP_STRING_DOUBLE:

                                    int lenMapStringDouble = byteBuffer.getInt();
                                    Map<String, Double> mapStringDoubleValue = new HashMap<>(lenMapStringDouble);

                                    if (lenMapStringDouble > 0) {
                                        String[] keyArray = decodeStringArray(byteBuffer, lenMapStringDouble);


                                        double[] values = new double[lenMapStringDouble];
                                        for (int j = 0; j < lenMapStringDouble; j++) {
                                            values[j] = byteBuffer.getDouble();
                                        }
                                        for (int idx = 0; idx < lenMapStringDouble; idx++) {
                                            mapStringDoubleValue.put(keyArray[idx], values[idx]);
                                        }
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, mapStringDoubleValue);
                                    }
                                    break;
                                case FS_MAP_STRING_STRING:

                                    int length = byteBuffer.getInt();
                                    Map<String, String> mapStringStringValue = new HashMap<>(length);

                                    if (length > 0) {
                                        String[] keyArray = decodeStringArray(byteBuffer, length);
                                        String[] values = decodeStringArray(byteBuffer, length);

                                        for (int idx = 0; idx < length; idx++) {
                                            mapStringStringValue.put(keyArray[idx], values[idx]);
                                        }
                                    }
                                    if (selectFieldSet.contains(featureName)) {
                                        featureMap.put(featureName, mapStringStringValue);
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
        String[] selectFields = null;
        if (!StringUtils.isEmpty(featureViewSeqConfig.getPlayTimeField())) {
            selectFields = new String[]{featureViewSeqConfig.getItemIdField(), featureViewSeqConfig.getEventField(), featureViewSeqConfig.getPlayTimeField(), featureViewSeqConfig.getTimestampField()};
        } else {
            selectFields = new String[]{featureViewSeqConfig.getItemIdField(), featureViewSeqConfig.getEventField(), featureViewSeqConfig.getTimestampField()};
        }
        long currentime = System.currentTimeMillis() / 1000;
        HashMap<String, Double> playtimefilter = new HashMap<>();
        if (!StringUtils.isEmpty(featureViewSeqConfig.getPlayTimeFilter())) {
            for (String event : Strings.split(featureViewSeqConfig.getPlayTimeFilter(), ';')) {
                String[] s = Strings.split(event, ':');
                if (s.length == 2) {//key有值
                    playtimefilter.put(s[0], Double.valueOf(s[1]));
                }
            }
        }
        String[] events = new String[featureViewSeqConfig.getSeqConfigs().length];
        for (int i = 0; i < events.length; i++) {
            events[i] = featureViewSeqConfig.getSeqConfigs()[i].getSeqEvent();
        }
        Set<String> featureFieldList = new HashSet<>();
        List<Map<String, Object>> featureDataList = new ArrayList<>();
        for (String key : keys) {
            HashMap<String, String> keyEventsDatasOnline = new HashMap<>();
            for (String event : events) {
                List<SequenceInfo> onlineSequence = fetchData(key, playtimefilter, featureViewSeqConfig, event, userIdField, currentime, true);
                List<SequenceInfo> offlineSequence = new ArrayList<>();
                onlineSequence = MergeOnOfflineSeq(onlineSequence, offlineSequence, featureViewSeqConfig, event);
                Map<String, String> resultData = disposeDB(onlineSequence, selectFields, featureViewSeqConfig, event, currentime);
                if (onlineSequence.size() > 0) {
                    keyEventsDatasOnline.putAll(resultData);
                }

            }

            if (keyEventsDatasOnline.size() > 0) {
                keyEventsDatasOnline.put(this.primaryKeyField, key);
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
        int f = 0;
        for (String field : featureFieldList) {
            fields[f++] = field;
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
        this.executor.submit(() -> {
            try {
                this.featureDBClient.writeFeatureDB(tempList, this.database, this.schema, this.table);
            } catch (Exception e) {
                log.error(String.format("request featuredb error:%s", e.getMessage()));
            }
        });
    }

    public List<SequenceInfo> fetchData(String key, HashMap<String, Double> playtimefilter,
                                        FeatureViewSeqConfig config, String event, String userIdFields, Long currentime, boolean useOnlineTable) {
        List<SequenceInfo> sequenceInfos = new ArrayList<>();
        List<String> pks = new ArrayList<>();
        for (String e : event.split("\\|")) {
            pks.add(String.format("%s\u001D%s", key, e));
        }
        try {
            byte[] content = this.featureDBClient.kkvRequestFeatureDB(pks, this.database, this.schema, this.table, config.getSeqLenOnline());
            if (content != null) {
                KKVRecordBlock kkvRecordBlock = KKVRecordBlock.getRootAsKKVRecordBlock(ByteBuffer.wrap(content));
                for (int i = 0; i < kkvRecordBlock.valuesLength(); i++) {
                    KKVData kkvData = new KKVData();
                    kkvRecordBlock.values(kkvData, i);
                    String pk = kkvData.pk();
                    String[] userIdEvent = pk.split("\u001D");
                    if (userIdEvent.length != 2) {
                        continue;
                    }
                    String itemId = "";
                    if (config.getDeduplicationMethodNum() == 1) {
                        itemId = kkvData.sk();
                    } else if (config.getDeduplicationMethodNum() == 2) {
                        String sk = kkvData.sk();
                        String[] itemIdTimestamp = sk.split("\u001D");
                        if (itemIdTimestamp.length != 2) {
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
                    if (Objects.equals(sequenceInfo.getEventField(), "") || sequenceInfo.getItemIdField() == 0) {
                        continue;

                    }
                    if (playtimefilter.containsKey(sequenceInfo.getEventField())) {
                        double t = playtimefilter.get(sequenceInfo.getEventField());
                        if (sequenceInfo.getPlayTimeField() <= t) {
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

    public List<SequenceInfo> MergeOnOfflineSeq(List<SequenceInfo> offlineSequence, List<SequenceInfo> onlineSequence, FeatureViewSeqConfig config, String event) {

        if (offlineSequence.isEmpty()) {
            return onlineSequence;
        } else if (onlineSequence.isEmpty()) {
            return offlineSequence;
        } else {
            int index = 0;
            for (; index < onlineSequence.size(); ) {
                if (Long.valueOf(onlineSequence.get(index).getTimestampField()) < Long.valueOf(offlineSequence.get(0).getTimestampField())) {
                    break;
                }
                index++;
            }
            onlineSequence = onlineSequence.subList(0, index);
            onlineSequence.addAll(offlineSequence);
            if (onlineSequence.size() > config.getSeqLenOnline()) {
                onlineSequence.subList(0, config.getSeqLenOnline());
            }

        }
        return onlineSequence;
    }

    public Map<String, String> disposeDB(List<SequenceInfo> sequenceInfos, String[] selectFields, FeatureViewSeqConfig config, String event, Long currentime) {
        HashMap<String, String> sequenceFeatures = new HashMap<>();
        for (SequenceInfo sequenceInfo : sequenceInfos) {
            String qz = "";
            for (SeqConfig s : config.getSeqConfigs()) {
                if (s.getSeqEvent().equals(event)) {
                    qz = s.getOnlineSeqName();
                    break;
                }
            }
            for (String name : selectFields) {
                String newname = qz + "__" + name;

                if (name.equals(config.getItemIdField())) {
                    if (sequenceFeatures.containsKey(newname)) {
                        sequenceFeatures.put(newname, sequenceFeatures.get(newname) + ";" + sequenceInfo.getItemIdField());
                    } else {
                        sequenceFeatures.put(newname, "" + sequenceInfo.getItemIdField());
                    }
                    if (sequenceFeatures.containsKey(qz)) {
                        sequenceFeatures.put(qz, sequenceFeatures.get(qz) + ";" + sequenceInfo.getItemIdField());
                    } else {
                        sequenceFeatures.put(qz, "" + sequenceInfo.getItemIdField());
                    }
                } else if (name.equals(config.getTimestampField())) {
                    if (sequenceFeatures.containsKey(newname)) {
                        sequenceFeatures.put(newname, sequenceFeatures.get(newname) + ";" + sequenceInfo.getTimestampField());
                    } else {
                        sequenceFeatures.put(newname, "" + sequenceInfo.getTimestampField());
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
                        sequenceFeatures.put(newname, "" + sequenceInfo.getPlayTimeField());
                    }

                }
            }
            String tsfields = qz + "__ts";//Timestamp from the current time
            long eventTime = 0;
            if (!StringUtils.isEmpty(sequenceInfo.getTimestampField())) {
                eventTime = Long.valueOf(sequenceInfo.getTimestampField());
            }
            if (sequenceFeatures.containsKey(tsfields)) {
                sequenceFeatures.put(tsfields, sequenceFeatures.get(tsfields) + ";" + (currentime - eventTime));
            } else {
                sequenceFeatures.put(tsfields, String.valueOf((currentime - eventTime)));


            }
        }

        return sequenceFeatures;
    }

    private String[] decodeStringArray(ByteBuffer byteBuffer, int length) {
        String[] arrayStringValue = new String[length];
        if (length > 0) {
            int[] offsets = new int[length + 1];
            for (int i = 0; i <= length; i++) {
                offsets[i] = byteBuffer.getInt();
            }

            int totalLength = offsets[length];
            byte[] stringData = new byte[totalLength];
            byteBuffer.get(stringData);

            for (int strIdx = 0; strIdx < length; strIdx++) {
                int start = offsets[strIdx];
                int end = offsets[strIdx + 1];
                arrayStringValue[strIdx] = new String(stringData, start, end - start, StandardCharsets.UTF_8);
            }
        }
        return arrayStringValue;
    }
}
