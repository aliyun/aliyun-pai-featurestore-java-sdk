package com.aliyun.openservices.paifeaturestore.datasource;

import com.alicloud.openservices.tablestore.SyncClient;

import java.util.HashMap;
import java.util.Map;

public class TableStoreFactory {
    private static Map<String, SyncClient> tablestoreMap = new HashMap<>();

    public static void register(String name, SyncClient otsClient) {
        if (!tablestoreMap.containsKey(name)) {
            tablestoreMap.put(name, otsClient);
        }
    }

    public static SyncClient get(String name) {
        return tablestoreMap.get(name);
    }
}
