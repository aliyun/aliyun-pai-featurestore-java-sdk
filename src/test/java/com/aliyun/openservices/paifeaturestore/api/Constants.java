package com.aliyun.openservices.paifeaturestore.api;

import org.junit.Ignore;

@Ignore
public class Constants {
    public static String accessId = "";
    public static String accessKey = "";
    public static String host = "paifeaturestore.cn-beijing.aliyuncs.com";

    public static Boolean usePublicAddress = true;

    public static String username = "";

    public static String password = "";
    static {
        accessId = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
        accessKey = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
        username = System.getenv("FEATUREDB_USERNAME");
        password = System.getenv("FEATUREDB_PASSWORD");
    }
}
