package com.aliyun.openservices.paifeaturestore.api;
import com.aliyun.teaopenapi.models.Config;

public class Configuration {
    private String projectName;

    private String domain = null;

    private Config config;

    public Configuration(String regionId, String accessKeyId, String accessKeySecret, String projectName) {
        this.config = new Config();
        this.config.setAccessKeyId(accessKeyId);
        this.config.setAccessKeySecret(accessKeySecret);
        this.config.setType("access_key");
        this.config.setRegionId(regionId);
        this.projectName = projectName;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getDomain() {
        if (this.domain == null) {
            this.domain = "paifeaturestore-vpc." +this.config.getRegionId() + ".aliyuncs.com";
        }
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public Config getConfig() {
        this.config.setEndpoint(this.getDomain());
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }
}
