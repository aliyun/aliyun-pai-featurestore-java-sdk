package com.aliyun.openservices.paifeaturestore.api;

import org.junit.Ignore;

@Ignore
public class Constants {
    public static String accessId = "";
    public static String accessKey = "";
    public static String host = "paifeaturestore-vpc.cn-shenzhen.aliyuncs.com";

    public static Boolean usePublicAddress = true;

    public static String username = "";

    public static String password = "";
    static {
        accessId = System.getenv("ACCESS_ID");
        accessKey = System.getenv("ACCESS_KEY");
        username = System.getenv("FEATUREDB_USERNAME");
        password = System.getenv("FEATUREDB_PASSWORD");
    }
}
