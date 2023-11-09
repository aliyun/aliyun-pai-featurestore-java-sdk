package com.aliyun.openservices.paifeaturestore.dao;

import com.aliyun.igraph.client.gremlin.driver.Client;
import com.aliyun.openservices.paifeaturestore.StringUtil;
import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.aliyun.openservices.paifeaturestore.datasource.IGraphFactory;
import com.aliyun.openservices.paifeaturestore.domain.FeatureResult;
import com.aliyun.openservices.paifeaturestore.domain.FeatureStoreResult;
import com.aliyun.tea.utils.StringUtils;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        for (Map.Entry<String, String> entry : this.fieldMap.entrySet()) {
           this.reverseFieldMap.put(entry.getValue(), entry.getKey());
           this.reverseFieldTypeMap.put(entry.getValue(), this.fieldTypeMap.get(entry.getKey()));
        }
    }

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
                        String str = result.getVertex().value(igraphField);
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
}
