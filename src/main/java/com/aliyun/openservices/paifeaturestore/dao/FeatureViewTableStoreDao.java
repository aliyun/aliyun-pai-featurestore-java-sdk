package com.aliyun.openservices.paifeaturestore.dao;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.BatchGetRowRequest;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.MultiRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.aliyun.openservices.paifeaturestore.datasource.TableStoreFactory;
import com.aliyun.openservices.paifeaturestore.domain.FeatureResult;
import com.aliyun.openservices.paifeaturestore.domain.FeatureStoreResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FeatureViewTableStoreDao implements FeatureViewDao{
    private SyncClient syncClient;
    private String table;
    private String primaryKeyField;
    private String eventTimeField;
    int ttl;

    public Map<String , FSType> fieldTypeMap ;
    public FeatureViewTableStoreDao(DaoConfig daoConfig) {
        this.table = daoConfig.otsTableName;
        this.ttl = daoConfig.ttl;
        this.primaryKeyField = daoConfig.primaryKeyField;
        this.eventTimeField = daoConfig.eventTimeField;

        SyncClient client = TableStoreFactory.get(daoConfig.otsName);
        if (null == client) {
            throw  new RuntimeException(String.format("otsclient:%s not found", daoConfig.otsName));
        }
        this.syncClient = client;
        this.fieldTypeMap = daoConfig.fieldTypeMap;

    }

    @Override
    public FeatureResult getFeatures(String[] keys, String[] selectFields) {
        List<Map<String ,Object>> featureDataList = new ArrayList<>();
        BatchGetRowRequest batchGetRowRequest = new BatchGetRowRequest();
        MultiRowQueryCriteria multiRowQueryCriteria = new MultiRowQueryCriteria(this.table);
        for (String key : keys) {
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            switch (this.fieldTypeMap.get(this.primaryKeyField)) {
                case FS_STRING:
                    primaryKeyBuilder.addPrimaryKeyColumn(this.primaryKeyField, PrimaryKeyValue.fromString(key));
                    break;
                case FS_INT32:
                case FS_INT64:
                    primaryKeyBuilder.addPrimaryKeyColumn(this.primaryKeyField, PrimaryKeyValue.fromLong(Long.valueOf(key)));
                    break;
                default:
                    throw new RuntimeException("primary key type is not supported by OTS");
            }

            multiRowQueryCriteria.addRow(primaryKeyBuilder.build());
            multiRowQueryCriteria.setMaxVersions(1);
            multiRowQueryCriteria.addColumnsToGet(selectFields);
        }

        multiRowQueryCriteria.setTableName(this.table);
        batchGetRowRequest.addMultiRowQueryCriteria(multiRowQueryCriteria);

        BatchGetRowResponse getRowResponse = this.syncClient.batchGetRow(batchGetRowRequest);

        for (BatchGetRowResponse.RowResult rowResult : getRowResponse.getBatchGetRowResult(this.table)) {
            Map<String, Object> featureMap = new HashMap<>();
            switch (this.fieldTypeMap.get(this.primaryKeyField)) {
                case FS_STRING:
                    featureMap.put(this.primaryKeyField, rowResult.getRow().getPrimaryKey().getPrimaryKeyColumn(this.primaryKeyField).getValue().asString());
                    break;
                case FS_INT32:
                case FS_INT64:
                    featureMap.put(this.primaryKeyField, rowResult.getRow().getPrimaryKey().getPrimaryKeyColumn(this.primaryKeyField).getValue().asLong());
                    break;
            }

            for (String featureName : selectFields) {
                List<Column> columns = rowResult.getRow().getColumn(featureName);
                if (columns == null || columns.size() == 0) {
                    continue;
                }
                switch (this.fieldTypeMap.get(featureName)) {
                    case FS_STRING:
                    case FS_TIMESTAMP:
                        featureMap.put(featureName, columns.get(0).getValue().asString());
                        break;
                    case FS_FLOAT:
                    case FS_DOUBLE:
                        featureMap.put(featureName, columns.get(0).getValue().asDouble());
                        break;
                    case FS_INT32:
                    case FS_INT64:
                        featureMap.put(featureName, columns.get(0).getValue().asLong());
                        break;
                    case FS_BOOLEAN:
                        featureMap.put(featureName, columns.get(0).getValue().asBoolean());
                        break;
                }
            }

            featureDataList.add(featureMap);
        }

        FeatureStoreResult featureResult = new FeatureStoreResult();
        featureResult.setFeatureFields(selectFields);
        featureResult.setFeatureFieldTypeMap(this.fieldTypeMap);
        featureResult.setFeatureDataList(featureDataList);

        return featureResult;
    }
}
