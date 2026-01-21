package com.aliyun.openservices.paifeaturestore.dao;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.aliyun.openservices.paifeaturestore.datasource.TableStoreFactory;
import com.aliyun.openservices.paifeaturestore.domain.FeatureResult;
import com.aliyun.openservices.paifeaturestore.domain.FeatureStoreResult;
import com.aliyun.openservices.paifeaturestore.model.FeatureViewSeqConfig;
import com.aliyun.openservices.paifeaturestore.model.SeqConfig;
import com.aliyun.openservices.paifeaturestore.model.SequenceInfo;
import com.aliyun.tea.utils.StringUtils;
import org.bouncycastle.util.Strings;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/*  This class defines operations related to TableStore(ots) data source access characteristics.*/
public class FeatureViewTableStoreDao extends AbstractFeatureViewDao {
    private SyncClient syncClient;
    private String table;
    private String primaryKeyField;
    private String eventTimeField;
    int ttl;

    public Map<String, FSType> fieldTypeMap;
    private String offlinetable;
    private String onlinetable;

    public FeatureViewTableStoreDao(DaoConfig daoConfig) {
        this.table = daoConfig.otsTableName;
        this.ttl = daoConfig.ttl;
        this.primaryKeyField = daoConfig.primaryKeyField;
        this.eventTimeField = daoConfig.eventTimeField;

        SyncClient client = TableStoreFactory.get(daoConfig.otsName);
        if (null == client) {
            throw new RuntimeException(String.format("otsclient:%s not found", daoConfig.otsName));
        }
        this.syncClient = client;
        this.fieldTypeMap = daoConfig.fieldTypeMap;
        this.onlinetable=daoConfig.otsSeqOnlineTableName;
        this.offlinetable=daoConfig.otsSeqOfflineTableName;

    }

    @Override
    public FeatureResult getFeatures(String[] keys, String[] selectFields) {
        List<String> group = Arrays.asList(keys);


        FeatureStoreResult featureResult = (FeatureStoreResult) this.doGetFeatures(group, selectFields);
        featureResult.setFeatureFields(selectFields);
        featureResult.setFeatureFieldTypeMap(this.fieldTypeMap);

        return featureResult;
    }
    public FeatureResult doGetFeatures(List<String> keys, String[] selectFields) {
        List<Map<String, Object>> featureDataList = new ArrayList<>();
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
            if (!rowResult.isSucceed() || rowResult.getRow() == null ) {
                continue;
            }
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
        //featureResult.setFeatureFields(selectFields);
        //featureResult.setFeatureFieldTypeMap(this.fieldTypeMap);
        featureResult.setFeatureDataList(featureDataList);

        return featureResult;
    }

    @Override
    public FeatureResult getSequenceFeatures(String[] keys, String userIdField, FeatureViewSeqConfig config, SeqConfig[] seqConfigs) {
        FeatureStoreResult featureStoreResult = new FeatureStoreResult();

        String[] selectFields=null;
        if (!StringUtils.isEmpty(config.getPlayTimeField())) {
            selectFields=new String[]{config.getItemIdField(),config.getEventField(),config.getPlayTimeField(),config.getTimestampField()};
        } else {
            selectFields=new String[]{config.getItemIdField(),config.getEventField(),config.getTimestampField()};
        }

        HashMap<String, Double> playtimefilter = new HashMap<>();
        if (!StringUtils.isEmpty(config.getPlayTimeFilter())) {
            for (String event:Strings.split(config.getPlayTimeFilter(), ';')) {
                String[] s = Strings.split(event, ':');
                if (s.length==2) {
                    playtimefilter.put(s[0], Double.valueOf(s[1]));
                }
            }
        }

        long currentime=System.currentTimeMillis();

        String pkField = String.format("%s_%s", userIdField, config.getEventField());
        String skFiled="";
        if (config.getDeduplicationMethodNum()==1) {
            skFiled=""+config.getItemIdField();//item_id
        } else if (config.getDeduplicationMethodNum()==2) {
            skFiled=String.format("%s_%s",config.getItemIdField(),config.getTimestampField());//item_id_event_time
        }
        Set<String> featureFieldList = new HashSet<>();
        List<Map<String, Object>> featureDataList = new ArrayList<>();


        String[] events=new String[config.getSeqConfigs().length];
        for (int i=0;i<events.length;i++) {
            events[i]=config.getSeqConfigs()[i].getSeqEvent();
        }

        for (String key:keys) {
            HashMap<String, String> keyEventsDatasOnline = new HashMap<>();
            for (String event : events) {
                //Traverse to get online data
                List<SequenceInfo> seqOnlineDB = getOtsSeqResult(key, config, selectFields, playtimefilter, pkField, skFiled, event, this.onlinetable, currentime, true);

                //Traverse to get offline data
                List<SequenceInfo> seqOfflineDB = getOtsSeqResult(key, config, selectFields, playtimefilter, pkField, skFiled, event, this.offlinetable, currentime, false);

                //merge
                seqOnlineDB = MergeOnOfflineSeq(seqOnlineDB, seqOfflineDB, config, event);

                Map<String, String> resultData = disposeDB(seqOnlineDB,selectFields,config,null, event,currentime);

                if (seqOnlineDB.size() > 0) {
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
                        for (Map.Entry<String,String> entry:keyEventsDatasOnline.entrySet()) {
                            features.put(entry.getKey(),entry.getValue());
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
        featureStoreResult.setFeatureFieldTypeMap(featureFieldTypeMap);
        featureStoreResult.setFeatureFieldTypeMap(this.fieldTypeMap);
        featureStoreResult.setFeatureFields(fields);
        featureStoreResult.setFeatureDataList(featureDataList);

        return featureStoreResult;
    }


    /* query Offline or Online  data. */
    public List<SequenceInfo> getOtsSeqResult(String key,FeatureViewSeqConfig config, String[] selectFields,HashMap<String, Double> playtimefilter,
                                              String pkField,String skField, String event, String tablename, Long currentime,boolean useOnlinetable) {
        ArrayList<SequenceInfo> sequenceInfos = new ArrayList<>();
        //Range-based search
        GetRangeRequest rangeRequest = new GetRangeRequest();
        //Bind query criteria
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tablename);

        PrimaryKeyBuilder startPK =PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPK.addPrimaryKeyColumn(pkField,PrimaryKeyValue.fromString(key+"_"+event));
        startPK.addPrimaryKeyColumn(skField,PrimaryKeyValue.INF_MIN);

        PrimaryKeyBuilder endPK =PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPK.addPrimaryKeyColumn(pkField,PrimaryKeyValue.fromString(key+"_"+event));
        endPK.addPrimaryKeyColumn(skField,PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setMaxVersions(1);
        //Binding starting range(pk,sk)
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPK.build());
        //End of binding range
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPK.build());
        rangeRowQueryCriteria.setDirection(Direction.FORWARD);
        rangeRowQueryCriteria.addColumnsToGet(selectFields);

        if (useOnlinetable) {
            rangeRowQueryCriteria.setTimeRange(new TimeRange((currentime-86400*5*1000),currentime));
        }

        rangeRequest.setRangeRowQueryCriteria(rangeRowQueryCriteria);
        //Get response result
        GetRangeResponse range = this.syncClient.getRange(rangeRequest);

        //Process the data and add it to the map
        if (StringUtils.isEmpty(range.getConsumedCapacity())) {
            System.out.println("get range don't have any data");
        } else {
            for (Row r:range.getRows()) {
                if (!StringUtils.isEmpty(playtimefilter) &&  !StringUtils.isEmpty(playtimefilter.get(event))) {
                    if (r.getLatestColumn(config.getPlayTimeField()).getValue().asDouble() < playtimefilter.get(event)) {
                        continue;
                    }
                }

                if (r.getPrimaryKey().equals(null)) {
                    continue;
                }

                if (r.getColumns().length==0) {
                    continue;
                }

                String qz="";
                String newname="";
                SequenceInfo sequenceInfo = new SequenceInfo();
                if (config.getDeduplicationMethodNum()==1) {//pk:item_id
                    for (SeqConfig s:config.getSeqConfigs()) {
                        if (s.getSeqEvent().equals(event)) {
                            qz=s.getOnlineSeqName();//click_5_seq
                            newname=s.getOnlineSeqName()+"_"+config.getItemIdField();//click_5_seq_item_id
                            break;
                        }
                    }
                    Long item_id= Long.valueOf(String.valueOf(r.getPrimaryKey().getPrimaryKeyColumn(1).getValue()));
                    sequenceInfo.setItemIdField(item_id);
                }

                for (Column c:r.getColumns()) {
                    String result=String.valueOf(c.getValue());
                    if (c.getName().equals(config.getItemIdField())) {
                        sequenceInfo.setItemIdField(Long.valueOf(result));
                    } else if (c.getName().equals(config.getEventField())) {
                        sequenceInfo.setEventField(result);
                    } else if (c.getName().equals(config.getPlayTimeField())) {
                        sequenceInfo.setPlayTimeField(Double.valueOf(result));
                    } else if (c.getName().equals(config.getTimestampField())) {
                        sequenceInfo.setTimestampField(Long.valueOf(result));
                    }
                }
                sequenceInfos.add(sequenceInfo);
            }
        }
        return sequenceInfos;
    }



}


