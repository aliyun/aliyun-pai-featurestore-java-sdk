package com.aliyun.openservices.paifeaturestore.dao;

import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.aliyun.openservices.paifeaturestore.datasource.Hologres;
import com.aliyun.openservices.paifeaturestore.datasource.HologresFactory;
import com.aliyun.openservices.paifeaturestore.domain.FeatureResult;
import com.aliyun.openservices.paifeaturestore.domain.FeatureStoreResult;
import com.aliyun.openservices.paifeaturestore.model.FeatureViewSeqConfig;

import com.aliyun.openservices.paifeaturestore.model.SeqConfig;
import com.aliyun.openservices.paifeaturestore.model.SequenceInfo;
import com.aliyun.tea.utils.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bouncycastle.util.Strings;
import org.jooq.*;
import org.jooq.impl.DSL;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.jooq.impl.DSL.*;


/*  This class defines operations related to Hologres data source access characteristics.*/
public class FeatureViewHologresDao extends AbstractFeatureViewDao{
    private static Log log = LogFactory.getLog(FeatureViewHologresDao.class);//日志工厂
    private DataSource datasource;
    private String table;
    private String primaryKeyField;
    private String eventTimeField;
    int ttl;
    public Map<String , FSType> fieldTypeMap ;
    private String offlinetable;
    private String onlinetable;

    /*  Initialization method
    * @Param daoConfig
    */
    public FeatureViewHologresDao(DaoConfig daoConfig) {
        this.table = daoConfig.hologresTableName;
        this.ttl = daoConfig.ttl;
        this.primaryKeyField = daoConfig.primaryKeyField;
        this.eventTimeField = daoConfig.eventTimeField;
        //  Gets the hologres object from the Hologres factory based on the Hologres data source name
        Hologres hologres = HologresFactory.get(daoConfig.hologresName);
        if (null == hologres) {
            throw  new RuntimeException(String.format("hologres:%s not found", daoConfig.hologresName));
        }
        this.datasource = hologres.getDataSource();
        this.fieldTypeMap = daoConfig.fieldTypeMap;
        this.onlinetable=daoConfig.hologresSeqOnlineTableName;
        this.offlinetable=daoConfig.hologresSeqOfflineTableName;
    }

    /*Gets a feature result set based on keys and selecting an array of fields to display.
    * @Param keys(@code String array)
    * @Param selectFields(@code String array)
    * @return FeatureResult*/
    @Override
    public FeatureResult getFeatures(String[] keys, String[] selectFields) {
        FeatureStoreResult featureResult = new FeatureStoreResult();
        List<Map<String, Object>> featuresList = new ArrayList<>();
        List<Field> fields = new ArrayList<>();
        for (String f : selectFields) {
            fields.add(field(String.format("\"%s\"", f)));//添加要显示的字段
        }

        DSLContext dsl = DSL.using(SQLDialect.POSTGRES);

        Query query=dsl
                    .select(fields)
                    .from(table(table))
                    .where(field(String.format("\"%s\"",this.primaryKeyField)).in(keys));

        String sql = query.getSQL();
        try (Connection connection = this.datasource.getConnection();
            PreparedStatement statement = connection.prepareStatement(sql)) {
            int pos = 1;
            for (String key : keys) {
                if (this.fieldTypeMap.get(this.primaryKeyField) == FSType.FS_STRING) {
                    statement.setString(pos++, key);
                } else if (this.fieldTypeMap.get(this.primaryKeyField) == FSType.FS_INT64) {
                    statement.setLong(pos++, Long.valueOf(key));
                } else if (this.fieldTypeMap.get(this.primaryKeyField) == FSType.FS_INT32) {
                    statement.setInt(pos++, Integer.valueOf(key));
                } else {
                    statement.setString(pos++, key);
                }
            }

            try (ResultSet result = statement.executeQuery()) {//Execute query statement
                while (result.next()) {
                    Map<String, Object> featureMap = new HashMap<>();
                    for (String featureName : selectFields) {
                        if (null == result.getObject(featureName)) {
                            featureMap.put(featureName, null);
                            continue;
                        }
                        switch (this.fieldTypeMap.get(featureName)) {
                            case FS_STRING:
                                featureMap.put(featureName, result.getString(featureName));
                                break;
                            case FS_FLOAT:
                                featureMap.put(featureName, result.getFloat(featureName));
                                break;
                            case FS_INT32:
                                featureMap.put(featureName, result.getInt(featureName));
                                break;
                            case FS_INT64:
                                featureMap.put(featureName, result.getLong(featureName));
                                break;
                            case FS_DOUBLE:
                                featureMap.put(featureName, result.getDouble(featureName));
                                break;
                            case FS_BOOLEAN:
                                featureMap.put(featureName, result.getBoolean(featureName));
                                break;
                            case FS_TIMESTAMP:
                                featureMap.put(featureName, result.getTimestamp(featureName));
                                break;
                        }

                    }

                    featuresList.add(featureMap);
                }
            }
        } catch (Exception e) {
            log.error("getFeatures from hologres error", e);
            throw new RuntimeException(e);
        }
        featureResult.setFeatureFields(selectFields);
        featureResult.setFeatureFieldTypeMap(this.fieldTypeMap);
        featureResult.setFeatureDataList(featuresList);
        return featureResult;
    }

    /* Gets a result set of serialized feature fields based on keys.
    *  @Param keys(@code String array)
     * @Param userIdFields(@code String array)
     * @Param config,this class contains the feature view serialization feature configuration information.
     * @return FeatureResult*/
    @Override
    public FeatureResult getSequenceFeatures(String[] keys, String userIdFields, FeatureViewSeqConfig config, SeqConfig[] seqConfigs) {
        FeatureStoreResult featureStoreResult = new FeatureStoreResult();
        String[] selectFields=null;
        if (!StringUtils.isEmpty(config.getPlayTimeField())) {
            selectFields=new String[]{config.getItemIdField(),config.getEventField(),config.getPlayTimeField(),config.getTimestampField()};
        } else {
            selectFields=new String[]{config.getItemIdField(),config.getEventField(),config.getTimestampField()};
        }

        long currentime = System.currentTimeMillis()/1000; // unit: second


        HashMap<String, Double> seqPlayFilter = new HashMap<>();//<"click":10,"praise":5>
        if (!StringUtils.isEmpty(config.getPlayTimeFilter())) {
            for (String event:Strings.split(config.getPlayTimeFilter(), ';')) {//"click:10"
                String[] et=Strings.split(event,':');//{"click","10"}
                if (et.length==2) {
                    seqPlayFilter.put(et[0], Double.valueOf(et[1]));//<click,10>
                }
            }
        }

        //Traverse to get online data
        String[] events=new String[config.getSeqConfigs().length];
        for (int i=0;i<events.length;i++) {
            events[i]=config.getSeqConfigs()[i].getSeqEvent();
        }
        Set<String> featureFieldList = new HashSet<>();
        List<Map<String, Object>> featureDataList = new ArrayList<>();

        for (String key:keys) {
            HashMap<String, String> keyEventsDatasOnline = new HashMap<>();
            for (String event:events) {
                List<SequenceInfo> seqOnlineDB = getSeqDB(key, selectFields, seqPlayFilter, config, event, userIdFields, currentime, true);

                List<SequenceInfo> seqOfflineDB = getSeqDB(key, selectFields, seqPlayFilter, config, event, userIdFields, currentime, false);

                //merge
                seqOnlineDB=MergeOnOfflineSeq(seqOnlineDB,seqOfflineDB,config,event);

                Map<String, String> resultData = disposeDB(seqOnlineDB,selectFields,config,null, event, currentime);

                if (seqOnlineDB.size()>0) {
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

    /* query Offline or Online  data. */
    public List<SequenceInfo> getSeqDB(String key, String[] selectFields, HashMap<String, Double> seqPlayFilter,
                                        FeatureViewSeqConfig config, String event, String userIdFields, Long currentime, boolean useOnlineTable) {
        List<SequenceInfo> sequenceInfos = new ArrayList<>();
        ArrayList<Field> fields = new ArrayList<>();

        for (String f:selectFields) {
            fields.add(field(String.format("\"%s\"",f)));
        }
        String primaryKeyField = String.format("\"%s\"",this.primaryKeyField);

        DSLContext dsl=DSL.using(SQLDialect.POSTGRES);
        FeatureViewSeqConfig fsc = new FeatureViewSeqConfig();
        String table="";
        Query query=null;
        long nt=(currentime-86400*5);
        if (useOnlineTable) {
            table=onlinetable;
            query=dsl.select(fields)
                    .from(table)
                    .where(field(primaryKeyField).eq(key)).and(field(config.getEventField()).eq(event)).and(field(config.getTimestampField()).greaterThan(nt))
                    .orderBy(field(config.getTimestampField()).desc())
                    .limit(config.getSeqLenOnline());
        } else {
            table=offlinetable;
            query=dsl.select(fields)
                    .from(offlinetable)
                    .where(field(primaryKeyField).eq(key)).and(field(config.getEventField()).eq(event))
                    .orderBy(field(config.getTimestampField()).desc())
                    .limit(config.getSeqLenOnline());
        }

        String sql=query.getSQL();
        try (Connection connection=this.datasource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            int index = 1;
            if (this.fieldTypeMap.get(this.primaryKeyField) == FSType.FS_STRING) {
                preparedStatement.setString(index++, key);
            }else if (this.fieldTypeMap.get(this.primaryKeyField) == FSType.FS_INT32) {
                preparedStatement.setInt(index++, Integer.parseInt(key));
            }else if (this.fieldTypeMap.get(this.primaryKeyField) == FSType.FS_INT64){
                preparedStatement.setLong(index++, Long.parseLong(key));
            }

            preparedStatement.setString(index++, event);
            if (useOnlineTable) {
                preparedStatement.setLong(index++, nt);
                preparedStatement.setInt(index++,config.getSeqLenOnline());
            } else {
                preparedStatement.setInt(index++,config.getSeqLenOnline());
            }

            try (ResultSet rs = preparedStatement.executeQuery()) {
                String qz="";
                for (SeqConfig s:config.getSeqConfigs()) {
                    if (s.getSeqEvent().equals(event)) {
                        qz=s.getOnlineSeqName();
                        break;
                    }
                }

                while (rs.next()) {
                    if (!StringUtils.isEmpty(seqPlayFilter) &&  !StringUtils.isEmpty(seqPlayFilter.get(event))) {
                        if (rs.getDouble(config.getPlayTimeField()) < seqPlayFilter.get(event)) {
                            continue;
                        }
                    }
                    SequenceInfo sequenceInfo = new SequenceInfo();
                    for (String name:selectFields) {
                        if (name.equals(config.getItemIdField())) {
                            sequenceInfo.setItemIdField(rs.getString(name));
                        } else if (name.equals(config.getEventField())) {
                            sequenceInfo.setEventField(rs.getString(name));
                        } else if (name.equals(config.getPlayTimeField())) {
                            sequenceInfo.setPlayTimeField(Double.valueOf(rs.getString(name)));
                        } else if (name.equals(config.getTimestampField())) {
                            sequenceInfo.setTimestampField(Long.valueOf(rs.getString(name)));
                        }
                    }
                    sequenceInfos.add(sequenceInfo);

                }

            }
        } catch (Exception e) {
            log.error("getSequenceFeatures from hologres error", e);
            throw new RuntimeException(e);
        }
        return sequenceInfos;
    }






}