package com.aliyun.openservices.paifeaturestore.dao;

import com.aliyun.openservices.paifeaturestore.constants.DatasourceType;
import com.aliyun.openservices.paifeaturestore.constants.FSType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DaoConfig {
    public DatasourceType datasourceType;
    public String config;
    public String tableStoreName;
    public String tableStoreTableName;
    public String primaryKeyField;
    public String eventTimeField;
    public int  ttl = -1;

    // hologres
    public String hologresName;
    public String hologresTableName;

    //ots
    public String otsName;
    public String otsTableName;


    // igraph
    public String iGraphName;
    public String groupName;
    public String labelName;
    public boolean saveOriginalField = false;

    public Map<String, String> fieldMap = new HashMap<>();
    // redis, ots
    public Map<String , FSType> fieldTypeMap = new HashMap<>();

    public List<String> fields = new ArrayList<>();
}
