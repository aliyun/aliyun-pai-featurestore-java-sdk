package com.aliyun.openservices.paifeaturestore.datasource;

import com.alicloud.openservices.tablestore.core.utils.IOUtils;

import java.util.HashMap;
import java.util.Map;

public class FeatureDBFactory {
    private static Map<String, FeatureDBClient> featureDBClientHashMap = new HashMap<>();

    public static void register(String name, FeatureDBClient featureDBClient) {
        if (!featureDBClientHashMap.containsKey(name)) {
            featureDBClientHashMap.put(name, featureDBClient);
        }
    }

    public static FeatureDBClient get(String name) {
        return featureDBClientHashMap.get(name);
    }
}
