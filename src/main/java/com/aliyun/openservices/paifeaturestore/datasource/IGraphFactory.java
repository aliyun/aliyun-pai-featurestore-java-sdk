package com.aliyun.openservices.paifeaturestore.datasource;

import com.aliyun.igraph.client.gremlin.driver.Client;

import java.util.HashMap;
import java.util.Map;

public class IGraphFactory {
    private static Map<String, Client> igraphMap = new HashMap<>();

    public static void register(String name, Client otsClient) {
        if (!igraphMap.containsKey(name)) {
            igraphMap.put(name, otsClient);
        }
    }

    public static Client get(String name) {
        return igraphMap.get(name);
    }
}
