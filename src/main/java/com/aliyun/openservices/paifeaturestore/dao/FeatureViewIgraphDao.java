package com.aliyun.openservices.paifeaturestore.dao;

import com.aliyun.igraph.client.gremlin.driver.Client;
import com.aliyun.openservices.paifeaturestore.model.SeqConfig;
import com.aliyun.openservices.paifeaturestore.model.SequenceInfo;
import com.aliyun.openservices.paifeaturestore.util.StringUtil;
import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.aliyun.openservices.paifeaturestore.datasource.IGraphFactory;
import com.aliyun.openservices.paifeaturestore.domain.FeatureResult;
import com.aliyun.openservices.paifeaturestore.domain.FeatureStoreResult;
import com.aliyun.openservices.paifeaturestore.model.FeatureViewSeqConfig;
import com.aliyun.tea.utils.StringUtils;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.bouncycastle.util.Strings;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;

import static org.jooq.impl.DSL.field;

/*  This class defines operations related to Igraph data source access characteristics.*/
public class FeatureViewIgraphDao implements FeatureViewDao{
    private Client client;

    private String primaryKeyField;
    private String eventTimeField;
    int ttl;

    // igraph label field name : fstype
    private Map<String , FSType> fieldTypeMap ;

    // igraph label field name : featureview feature name
    private Map<String , String> fieldMap ;

    // featureview feature name : igraph label field name
    private Map<String , String> reverseFieldMap = new HashMap<>();

    private Map<String , FSType> reverseFieldTypeMap = new HashMap<>();

    String group;

    String label;

    String edgename;

    public FeatureViewIgraphDao(DaoConfig daoConfig) {
        this.ttl = daoConfig.ttl;
        this.primaryKeyField = daoConfig.primaryKeyField;
        this.eventTimeField = daoConfig.eventTimeField;

        Client client = IGraphFactory.get(daoConfig.iGraphName);
        if (null == client) {
            throw  new RuntimeException(String.format("igraph client:%s not found", daoConfig.iGraphName));
        }
        this.client = client;
        this.fieldTypeMap = daoConfig.fieldTypeMap;
        this.fieldMap = daoConfig.fieldMap;
        this.group = daoConfig.groupName;
        this.label = daoConfig.labelName;
        this.edgename=daoConfig.igraphEdgeName;
        for (Map.Entry<String, String> entry : this.fieldMap.entrySet()) {
           this.reverseFieldMap.put(entry.getValue(), entry.getKey());
           this.reverseFieldTypeMap.put(entry.getValue(), this.fieldTypeMap.get(entry.getKey()));
        }
    }

    /*Gets a feature result set based on keys and selecting an array of fields to display.
     * @Param keys(@code String array)
     * @Param selectFields(@code String array)
     * @return FeatureResult*/
    @Override
    public FeatureResult getFeatures(String[] keys, String[] selectFields) {
        List<Map<String, Object>> featureDataList = new ArrayList<>();

        List<String> selector = new ArrayList<>(selectFields.length);

        for (String field : selectFields) {
            selector.add(String.format("\"%s\"",  this.reverseFieldMap.get(field)));
        }

        Map<String, Object> bind = new HashMap<>();
        String queryString = null;
        if (this.fieldMap.size() == selectFields.length) {
            queryString = String.format("g(\"%s\").V($1).hasLabel(\"%s\")", this.group, this.label);
            bind.put("$1", StringUtil.join(keys, ";"));
        } else {
            queryString = String.format("g(\"%s\").V($2).hasLabel(\"%s\").fields(%s)", this.group, this.label, StringUtil.join(selector.toArray(new String[0]), ","));
            bind.put("$2", StringUtil.join(keys, ";"));
        }

        ResultSet resultSet = this.client.submit(queryString, bind);
        List<Result> resultList = resultSet.all().join();
        for (Result result : resultList) {
            Map<String, Object> featureMap = new HashMap<>();
            for (String field : selectFields) {
                String igraphField = this.reverseFieldMap.get(field);

                switch (this.fieldTypeMap.get(igraphField)) {
                    case FS_DOUBLE:
                    case FS_FLOAT:
                        Double val = result.getVertex().value(igraphField);
                        if (val == -1024.0) {
                            featureMap.put(field, null);
                        } else {
                            featureMap.put(field, val);
                        }
                        break;
                    case FS_INT32:
                    case FS_INT64:
                        Long lval = result.getVertex().value(igraphField);
                        if (lval == -1024L) {
                            featureMap.put(field, null);
                        } else {
                            featureMap.put(field, lval);
                        }
                        break;
                    default:
                        String str =result.getVertex().value(igraphField);
                        if (StringUtils.isEmpty(str)) {
                            featureMap.put(field, null);
                        } else {
                            featureMap.put(field, str);
                        }
                        break;
                }
            }
            featureDataList.add(featureMap);
        }

        FeatureStoreResult featureResult = new FeatureStoreResult();
        featureResult.setFeatureDataList(featureDataList);
        featureResult.setFeatureFields(selectFields);
        featureResult.setFeatureFieldTypeMap(this.reverseFieldTypeMap);
        return  featureResult;
    }

    /* Gets a result set of serialized feature fields based on keys.
     *  @Param keys(@code String array)
     * @Param userIdFields(@code String array)
     * @Param config,this class contains the feature view serialization feature configuration information.
     * @return FeatureResult*/
    @Override
    public FeatureResult getSequenceFeatures(String[] keys, String userIdField, FeatureViewSeqConfig config) {
        FeatureStoreResult featureStoreResult = new FeatureStoreResult();
        String[] selectFields=null;
        if (!StringUtils.isEmpty(config.getPlayTimeField())) {
            selectFields=new String[]{config.getItemIdField(),config.getEventField(),config.getPlayTimeField(),config.getTimestampField()};
        } else {
            selectFields=new String[]{config.getItemIdField(),config.getEventField(),config.getTimestampField()};
        }

        long currentime=System.currentTimeMillis()/1000;

        HashMap<String, Double> playtimefilter = new HashMap<>();
        if (!StringUtils.isEmpty(config.getPlayTimeFilter())) {
            for (String event:Strings.split(config.getPlayTimeFilter(), ';')) {
                String[] s = Strings.split(event, ':');
                if (s.length == 2) {//key有值
                    playtimefilter.put(s[0], Double.valueOf(s[1]));
                }
            }
        }

        String[] events=new String[config.getSeqConfigs().length];
        for (int i=0;i<events.length;i++) {
            events[i]=config.getSeqConfigs()[i].getSeqEvent();
        }

        Set<String> featureFieldList = new HashSet<>();
        List<Map<String, Object>> featureDataList = new ArrayList<>();

        //Traverse to get online data

        for (String key:keys) {
            HashMap<String, String> keyEventsDatasOnline = new HashMap<>();
            for (String event:events) {
                List<SequenceInfo> igraphData = getSeqDB(key, selectFields, playtimefilter, event, config, currentime);

                Map<String, String> resultDB = disposeDB(igraphData,selectFields,config,event,currentime);

                if (igraphData.size()>0) {
                    keyEventsDatasOnline.putAll(resultDB);
                }
            }
            if (keyEventsDatasOnline.size()>0) {
                keyEventsDatasOnline.put(this.primaryKeyField,key);
            }
            // TODO:: function params confuse
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
        featureStoreResult.setFeatureFields(fields);
        featureStoreResult.setFeatureFieldTypeMap(featureFieldTypeMap);
        featureStoreResult.setFeatureFieldTypeMap(this.fieldTypeMap);
        featureStoreResult.setFeatureDataList(featureDataList);
        return featureStoreResult;
    }

    /* query Offline or Online data. */
    public List<SequenceInfo> getSeqDB(String key,String[] selectfields,HashMap<String, Double> playtimefilter,String event,FeatureViewSeqConfig config,Long currentime) {
        List<SequenceInfo> sequenceInfos = new ArrayList<>();
        //pk
        String pKField=String.format("%s_%s",key,event);
        String queryString=String.format(
                "g(\"%s\").E(\"%s\").hasLabel(\"%s\").fields(\"%s\")" +
                ".order().by(\"%s\",Order.decr).limit(%d)",
                this.group,pKField,this.edgename,StringUtil.join(selectfields,";"),config.getTimestampField(),config.getSeqLenOnline());
        ResultSet rs = this.client.submit(queryString);
        List<Result> join = rs.all().join();

        for (Result r:join) {
              if (!StringUtils.isEmpty(playtimefilter) && !StringUtils.isEmpty(playtimefilter.get(event))) {
                Double playtime = r.getVertex().value(config.getPlayTimeField());
                if (playtime < playtimefilter.get(event)) {
                    continue;
                }
             }
              if (r.isNull()) {
                  continue;
              }
            SequenceInfo sequenceInfo = new SequenceInfo();

            for (String name:selectfields) {

                if (name.equals(config.getItemIdField())) {
                    Long i = r.getVertex().value(name);
                    sequenceInfo.setItemIdField(i);
                } else if (name.equals(config.getTimestampField())) {
                    Long et = r.getVertex().value(name);
                    sequenceInfo.setTimestampField(et);
                } else if (name.equals(config.getEventField())) {//event
                    String e = r.getVertex().value(name);
                    sequenceInfo.setEventField(e);
                } else if (name.equals(config.getPlayTimeField())) {
                    Double d = r.getVertex().value(name);
                    if (d==-1024.0) {
                         d=null;
                    }
                    sequenceInfo.setPlayTimeField(d);
                }
            }
            sequenceInfos.add(sequenceInfo);
        }
        return sequenceInfos;
    }

    /* query Offline or Online  data. */
    public Map<String,String> disposeDB(List<SequenceInfo> sequenceInfos, String[] selectFields, FeatureViewSeqConfig config, String event, Long currentime) {
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
                String newname = qz + "_" + name;

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
            String tsfields = qz + "_ts";//Timestamp from the current time
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
