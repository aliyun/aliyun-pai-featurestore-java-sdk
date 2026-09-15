package com.aliyun.openservices.paifeaturestore.flink.source;

import com.aliyun.openservices.paifeaturestore.FeatureStoreClient;
import com.aliyun.openservices.paifeaturestore.api.ApiClient;
import com.aliyun.openservices.paifeaturestore.api.Configuration;
import com.aliyun.openservices.paifeaturestore.domain.FeatureResult;
import com.aliyun.openservices.paifeaturestore.domain.FeatureView;
import com.aliyun.openservices.paifeaturestore.domain.Project;
import com.aliyun.tea.utils.StringUtils;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class FeatureStoreAsyncLookupFunction extends RichAsyncFunction<RowData, RowData>   implements ResultTypeQueryable<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(FeatureStoreAsyncLookupFunction.class);
    private String regionId;

    private String accessId;

    private String accessKey;

    private String project;

    private String featureViewName;

    private String username;

    private String password;

    private String host = null;

    private boolean usePublicAddress = false;

    private Long cacheSize;

    private Long cacheTime;
    private transient FeatureStoreClient featureStoreClient;

    private transient FeatureView featureView;
    private transient AsyncLoadingCache<String, RowData> cache; // 使用 transient 声明缓存

    protected InternalTypeInfo rowTypeInfo;

    private LogicalType primaryKeyLogicalType;

    private List<String> fieldNames;

    private List<LogicalType> fieldTypes;
    private String primaryKeyColumn;
    public FeatureStoreAsyncLookupFunction(String regionId, String accessId, String accessKey, String project, String featureViewName, String username, String password, String host, boolean usePublicAddress, Long cacheSize, Long cacheTime, TableSchema tableSchema) {
        this.regionId = regionId;
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.project = project;
        this.featureViewName = featureViewName;
        this.username = username;
        this.password = password;
        this.host = host;
        this.usePublicAddress = usePublicAddress;
        this.cacheSize = cacheSize;
        this.cacheTime = cacheTime;
        this.fieldNames = Arrays.asList(tableSchema.getFieldNames());
        if (tableSchema.getPrimaryKey().isPresent()) {
            List<String> primaryKeyColumns = tableSchema.getPrimaryKey().get().getColumns();
            LOG.info("Primary Key Columns: {}", primaryKeyColumns);
            if (primaryKeyColumns.size() != 1) {
                throw new RuntimeException("Primary Key Columns size must be 1");
            }

            this.primaryKeyColumn = primaryKeyColumns.get(0);
            for (String pkColumn : primaryKeyColumns) {
                DataType columnType = tableSchema.getTableColumn(pkColumn).get().getType();
                this.primaryKeyLogicalType = columnType.getLogicalType();
                break;
            }
        } else {
            throw new RuntimeException("Primary Key Columns not found");
        }

        this.fieldTypes = new ArrayList<>(this.fieldNames.size());
        for (int i = 0; i < this.fieldNames.size(); i++) {
            fieldTypes.add(tableSchema.getTableColumn(fieldNames.get(i)).get().getType().getLogicalType());
        }
        this.rowTypeInfo = InternalTypeInfo.of(tableSchema.toRowDataType().getLogicalType());
    }

    private void initializeFeatureView() {
        if (this.featureStoreClient == null || this.featureView == null) {
            Configuration configuration = new Configuration(regionId, accessId, accessKey, project);
            if (!StringUtils.isEmpty(host)) {
                configuration.setDomain(host);
            }
            configuration.setPassword(password);
            configuration.setUsername(username);

            ApiClient client = null;
            try {
                client = new ApiClient(configuration);
                this.featureStoreClient = new FeatureStoreClient(client, usePublicAddress);
                Project project1 =  this.featureStoreClient.getProject(project);
                if (null == project1) {
                    throw new RuntimeException(String.format("project:%s, not found", project));
                }
                this.featureView = project1.getFeatureView(featureViewName);
                if (null == featureView) {
                    throw new RuntimeException(String.format("featureview:%s, not found", featureViewName));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        this.initializeFeatureView();
        // 初始化缓存
        this.cache = Caffeine.newBuilder()
                .maximumSize(cacheSize) // 设置缓存最大条目数
                .expireAfterWrite(cacheTime, TimeUnit.SECONDS) // 设置写入后多久过期
                .buildAsync((key, executor) ->
                        // 异步加载逻辑：当缓存未命中时，此方法被调用
                        CompletableFuture.supplyAsync(() -> lookupFromFeatureStore(key), executor)
                );
    }
    /**
     * 从 Feature Store 查询数据的核心逻辑 (被缓存加载器调用)
     * @param joinId 查询的主键
     * @return 查询到的 RowData，如果未找到则返回 null
     */
    private RowData lookupFromFeatureStore(String joinId) {
        LOG.debug("Cache miss, looking up from FeatureStore for joinId: {}", joinId);
        try {
            FeatureResult featureResult = featureView.getOnlineFeatures(new String[]{joinId});

            if (featureResult != null && featureResult.getFeatureData().size() > 0) {
                featureResult.next();
                GenericRowData resultRow = new GenericRowData(fieldNames.size());
                for (int i = 0; i < fieldNames.size(); i++) {
                    String fieldName = fieldNames.get(i);
                    if (fieldName.equals(this.primaryKeyColumn)) { // primary key
                        String entityJoinId = featureView.getFeatureEntity().getFeatureEntity().getFeatureEntityJoinid();
                        switch (featureResult.getType(entityJoinId)) {
                            case FS_INT64:
                                resultRow.setField(i, featureResult.getLong(entityJoinId));
                                break;
                            case FS_STRING:
                                resultRow.setField(i, StringData.fromString(featureResult.getString(entityJoinId)));
                                break;
                            case FS_INT32:
                                resultRow.setField(i, featureResult.getInt(entityJoinId));
                                break;
                            default:
                                LOG.error("joinId:{},primary key type not support: {}", joinId, featureResult.getType(entityJoinId));
                                break;
                        }
                    } else {
                        if (featureResult.isNull(fieldName)) {
                            resultRow.setField(i, null);
                        } else {
                            if (this.fieldTypes.get(i) instanceof CharType || this.fieldTypes.get(i) instanceof VarCharType) {
                                resultRow.setField(i, StringData.fromString(featureResult.getString(fieldName)));
                            } else if (this.fieldTypes.get(i) instanceof BigIntType) {
                                resultRow.setField(i, featureResult.getLong(fieldName));
                            } else if (this.fieldTypes.get(i) instanceof IntType) {
                                resultRow.setField(i, featureResult.getInt(fieldName));
                            } else if (this.fieldTypes.get(i) instanceof FloatType) {
                                resultRow.setField(i, featureResult.getFloat(fieldName));
                            } else if (this.fieldTypes.get(i) instanceof DoubleType) {
                                resultRow.setField(i, featureResult.getDouble(fieldName));
                            } else if (this.fieldTypes.get(i) instanceof BooleanType) {
                                resultRow.setField(i, featureResult.getBoolean(fieldName));
                            } else if (this.fieldTypes.get(i) instanceof ArrayType) {
                                LogicalType fieldType = this.fieldTypes.get(i);
                                Object arrayObject = featureResult.getObject(fieldName);
                                if (arrayObject instanceof java.util.List) {
                                    java.util.List<?> list = (java.util.List<?>) arrayObject;
                                    LogicalType elementType = ((ArrayType) fieldType).getElementType();
                                    if (elementType instanceof VarCharType || elementType instanceof CharType) {
                                        Object[] stringDataArray = list.stream()
                                                .map(obj -> StringData.fromString(obj.toString()))
                                                .toArray();
                                        resultRow.setField(i, new GenericArrayData(stringDataArray));
                                    } else {
                                        // 对于其他元素类型（如INT, BIGINT等），可以直接 toArray()
                                        // 因为它们的Java包装类型 (Integer, Long) 可以被 Flink 内部正确处理
                                        resultRow.setField(i, new GenericArrayData(list.toArray()));
                                    }

                                } else if (arrayObject.getClass().isArray()) {
                                    // 如果返回的是原生数组 Object[]
                                    resultRow.setField(i, new GenericArrayData((Object[]) arrayObject));
                                } else {
                                    // 处理未预期的类型
                                    throw new IllegalArgumentException("Unsupported type for ArrayData conversion: " + arrayObject.getClass().getName());
                                }
                            } else {
                                LOG.debug("joinId:{},field: {}, type:{}, " , joinId, fieldName, this.fieldTypes.get(i));
                                resultRow.setField(i, featureResult.getObject(fieldName));
                            }
                        }
                    }
                }
                LOG.debug("Found result for joinId: {}, result: {}", joinId, resultRow);
                return resultRow;
            } else {
                // 如果外部系统没有找到数据，返回 null。Caffeine 会缓存这个 null 结果 (默认配置下)
                // 这可以防止对不存在的 key 进行重复查询（缓存穿透）
                LOG.debug("No result found in FeatureStore for joinId: {}", joinId);
                return null;
            }
        } catch (Exception e) {
            LOG.error("Error looking up from FeatureStore for joinId: " + joinId, e);
            // 发生异常时，不缓存结果，并让 CompletableFuture 以异常结束
            throw new RuntimeException(e);
        }
    }

    private String getJoinId(RowData input) {
        if (input == null) return null;
        if (this.primaryKeyLogicalType instanceof BigIntType) {
            return String.valueOf(input.getLong(0));
        } else if (this.primaryKeyLogicalType instanceof VarCharType || this.primaryKeyLogicalType instanceof CharType) {
            return input.getString(0).toString();
        } else if (this.primaryKeyLogicalType instanceof IntType) {
            return String.valueOf(input.getInt(0));
        }
        return null;
    }

    @Override
    public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) throws Exception {
        String joinId = getJoinId(input);
        if (joinId == null) {
            resultFuture.complete(Collections.emptyList());
            return;
        }
        CompletableFuture<RowData> future = cache.get(joinId);

        future.whenComplete((resultRow, throwable) -> {
            if (throwable != null) {
                // 如果在加载过程中发生异常
                LOG.error("Failed to lookup data for joinId: " + joinId, throwable);
                resultFuture.complete(Collections.emptyList());
            } else {
                if (resultRow != null) {
                    resultFuture.complete(Collections.singletonList(resultRow));
                } else {
                    resultFuture.complete(Collections.emptyList());
                }
            }
        });
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return this.rowTypeInfo;
    }
}
