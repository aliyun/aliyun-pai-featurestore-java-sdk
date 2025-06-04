package com.aliyun.openservices.paifeaturestore.api;
import com.aliyun.teaopenapi.models.Config;
/*  Configure the information class */
public class Configuration {
    private static final String CREDENTIAL_TYPE_ACCESS_KEY = "access_key";
    private String projectName;

    private String domain = null;

    private Config config;

    private String username = null;

    private String password = null;

    /*  Initial configuration information (region ID, AK account, AK password, and project name)    */
    public Configuration(String regionId, String accessKeyId, String accessKeySecret, String projectName) {
        this(regionId, accessKeyId, accessKeySecret, null, projectName);
    }
    public Configuration(String regionId, String accessKeyId, String accessKeySecret, String securityToken, String projectName) {
        initConfig(regionId, accessKeyId, accessKeySecret, securityToken);
        if (projectName == null) {
            throw new IllegalArgumentException("projectName must not be null");
        }
        this.projectName = projectName;
    }
    private void initConfig(String regionId, String accessKeyId, String accessKeySecret, String securityToken) {
        this.config = new Config();
        this.config.setAccessKeyId(accessKeyId);
        this.config.setAccessKeySecret(accessKeySecret);
        this.config.setType(CREDENTIAL_TYPE_ACCESS_KEY);
        this.config.setRegionId(regionId);
        if (securityToken != null) {
            this.config.setSecurityToken(securityToken);
        }
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

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
