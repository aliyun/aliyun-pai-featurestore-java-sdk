package com.aliyun.openservices.paifeaturestore.api;
import com.aliyun.teaopenapi.models.Config;
import com.aliyun.credentials.Client;

/*  Configure the information class */
public class Configuration {
    private static final String CREDENTIAL_TYPE_ACCESS_KEY = "access_key";
    private String projectName;

    private String domain = null;

    private Config config;

    private String username = null;

    private String password = null;

    private String hologresUsername = null;

    private String hologresPassword = null;

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
        this.config.setRegionId(regionId);
        
        if (accessKeyId != null && !accessKeyId.isEmpty() && 
            accessKeySecret != null && !accessKeySecret.isEmpty()) {
            this.config.setAccessKeyId(accessKeyId);
            this.config.setAccessKeySecret(accessKeySecret);
            this.config.setType(CREDENTIAL_TYPE_ACCESS_KEY);
            if (securityToken != null) {
                this.config.setSecurityToken(securityToken);
            }
        } else {
            Client client = new Client();
            this.config.setCredential(client);
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

    /**
     * 设置FeatureDB用户名
     * 
     * @param username FeatureDB用户名
     * @see #setPassword(String) 设置FeatureDB密码
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * 设置FeatureDB密码
     * 
     * @param password FeatureDB密码
     * @see #setUsername(String) 设置FeatureDB用户名
     */
    public void setPassword(String password) {
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    /**
     * 设置Hologres自定义用户的用户名
     * 
     * @param hologresUsername Hologres自定义用户的用户名
     * @see #setHologresPassword(String) 设置Hologres自定义用户的密码
     */
    public void setHologresUsername(String hologresUsername) {
        this.hologresUsername = hologresUsername;
    }

    /**
     * 设置Hologres数据库的密码
     * 
     * @param hologresPassword Hologres自定义用户的密码
     * @see #setHologresUsername(String) 设置Hologres自定义用户的用户名
     */
    public void setHologresPassword(String hologresPassword) {
        this.hologresPassword = hologresPassword;
    }

    public String getHologresUsername() {
        return hologresUsername;
    }

    public String getHologresPassword() {
        return hologresPassword;
    }
}
