package com.aliyun.openservices.paifeaturestore.datasource;

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

    public static void close() throws Exception{
        for (FeatureDBClient featureDBClient : featureDBClientHashMap.values()) {
            featureDBClient.close();
        }

        featureDBClientHashMap.clear();
    }
}
