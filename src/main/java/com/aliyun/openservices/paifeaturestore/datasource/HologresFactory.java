package com.aliyun.openservices.paifeaturestore.datasource;

import java.util.HashMap;
import java.util.Map;

public class HologresFactory {
    private static Map<String, Hologres> hologresMap = new HashMap<>();

    public static void register(String name, Hologres hologres) {
        if (!hologresMap.containsKey(name)) {
            hologresMap.put(name, hologres);
        }
    }

    public static Hologres get(String name) {
        return hologresMap.get(name);
    }
}
