package com.aliyun.openservices.paifeaturestore.api;

import org.junit.Ignore;

@Ignore
public class Constants {
    public static String accessId = "";
    public static String accessKey = "";
    public static String host = "paifeaturestore.cn-beijing.aliyuncs.com";

    static {
        accessId = System.getenv("AccessId");
        accessKey = System.getenv("AccessKey");
    }
}
