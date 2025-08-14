package com.aliyun.openservices.paifeaturestore.datasource;

public class HttpConfig {
    private int ioThreadNum;
    private int readTimeout;
    private int connectTimeout;
    private int requestTimeout;
    private int writeTimeout;
    private int maxConnectionCount;
    private int maxConnectionPerRoute;
    private boolean keepAlive;
    private boolean redirectsEnabled;
    private int keepAliveTimeout;


    public HttpConfig() {
        this.ioThreadNum = 10;
        this.readTimeout = 3000;
        this.writeTimeout = 3000;
        this.connectTimeout = 3000;
        this.maxConnectionCount = 1000;
        this.maxConnectionPerRoute = 1000;
        this.requestTimeout = 0;
        this.keepAlive = true;
        this.redirectsEnabled = false;
    }

    public HttpConfig(int ioThreadNum, int readTimeout, int connectTimeout,
                      int maxConnectionCount, int maxConnectionPerRoute) {
        super();
        this.ioThreadNum = ioThreadNum;
        this.readTimeout = readTimeout;
        this.connectTimeout = connectTimeout;
        this.maxConnectionCount = maxConnectionCount;
        this.maxConnectionPerRoute = maxConnectionPerRoute;
        this.keepAlive = true;
        this.redirectsEnabled = false;
    }

    public HttpConfig(int ioThreadNum, int readTimeout, int connectTimeout,
                      int maxConnectionCount, int maxConnectionPerRoute, int requestTimeout) {
        this(ioThreadNum, readTimeout, connectTimeout, maxConnectionCount, maxConnectionPerRoute);
        this.requestTimeout = requestTimeout;
    }



    public int getIoThreadNum() {
        return ioThreadNum;
    }

    public void setIoThreadNum(int ioThreadNum) {
        this.ioThreadNum = ioThreadNum;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getMaxConnectionCount() {
        return maxConnectionCount;
    }

    public void setMaxConnectionCount(int maxConnectionCount) {
        this.maxConnectionCount = maxConnectionCount;
    }

    public int getMaxConnectionPerRoute() {
        return maxConnectionPerRoute;
    }

    public void setMaxConnectionPerRoute(int maxConnectionPerRoute) {
        this.maxConnectionPerRoute = maxConnectionPerRoute;
    }

    public int getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(int requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    public int getWriteTimeout() {
        return writeTimeout;
    }

    public void setWriteTimeout(int writeTimeout) {
        this.writeTimeout = writeTimeout;
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    public int getKeepAliveTimeout() {
        return keepAliveTimeout;
    }

    public void setKeepAliveTimeout(int keepAliveTimeout) {
        this.keepAliveTimeout = keepAliveTimeout;
    }

    public boolean getRedirectsEnabled() {
        return redirectsEnabled;
    }

    public void setRedirectsEnabled(boolean redirectsEnabled) {
        this.redirectsEnabled = redirectsEnabled;
    }
}