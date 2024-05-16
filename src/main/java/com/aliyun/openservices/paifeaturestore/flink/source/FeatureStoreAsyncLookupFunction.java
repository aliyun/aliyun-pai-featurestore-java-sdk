package com.aliyun.openservices.paifeaturestore.flink.source;

import com.aliyun.openservices.paifeaturestore.FeatureStoreClient;
import com.aliyun.openservices.paifeaturestore.api.ApiClient;
import com.aliyun.openservices.paifeaturestore.api.Configuration;
import com.aliyun.openservices.paifeaturestore.domain.FeatureResult;
import com.aliyun.openservices.paifeaturestore.domain.FeatureView;
import com.aliyun.openservices.paifeaturestore.domain.Project;
import com.aliyun.tea.utils.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
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
    private transient FeatureStoreClient featureStoreClient;

    private transient FeatureView featureView;

    protected InternalTypeInfo rowTypeInfo;

    private LogicalType primaryKeyLogicalType;

    private List<String> fieldNames;

    private List<LogicalType> fieldTypes;
    private String primaryKeyColumn;
    public FeatureStoreAsyncLookupFunction(String regionId, String accessId, String accessKey, String project, String featureViewName, String username, String password, String host, boolean usePublicAddress, TableSchema tableSchema) {
        this.regionId = regionId;
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.project = project;
        this.featureViewName = featureViewName;
        this.username = username;
        this.password = password;
        this.host = host;
        this.usePublicAddress = usePublicAddress;
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
    }

    @Override
    public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) throws Exception {
        if (this.primaryKeyLogicalType != null && input != null) {
            String joinId = null;
            if (this.primaryKeyLogicalType instanceof BigIntType) {
                joinId = String.valueOf(input.getLong(0));
            } else if (this.primaryKeyLogicalType instanceof VarCharType || this.primaryKeyLogicalType instanceof CharType) {
                joinId = input.getString(0).toString();
            } else if (this.primaryKeyLogicalType instanceof IntType) {
                joinId = String.valueOf(input.getInt(0));
            }
            if (joinId != null) {
                LOG.debug("joinId:{}" , joinId);

                FeatureResult featureResult = null;
                try {
                    featureResult = featureView.getOnlineFeatures(new String[]{joinId});
                } catch (Exception e) {
                    LOG.error("getOnlineFeatures error:{}", e);
                    resultFuture.complete(Collections.emptyList());
                    return;
                }

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
                                    resultRow.setField(i, featureResult.getString(entityJoinId));
                                    break;
                                case FS_INT32:
                                    resultRow.setField(i, featureResult.getInt(entityJoinId));
                                    break;
                                default:
                                    LOG.error("primary key type not support: {}", featureResult.getType(entityJoinId));
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
                                } else {
                                    resultRow.setField(i, featureResult.getObject(fieldName));
                                }
                            }
                        }
                    }

                    LOG.debug("result: {}" , resultRow);
                    resultFuture.complete(Collections.singletonList(resultRow));
                } else {
                    resultFuture.complete(Collections.emptyList());
                }
            } else {
                resultFuture.complete(Collections.emptyList());
            }
        } else {
            resultFuture.complete(Collections.emptyList());
        }

    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return this.rowTypeInfo;
    }
}
