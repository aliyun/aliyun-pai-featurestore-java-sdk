package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.paifeaturestore20230621.Client;
/*
* This class is used to prepare for registering the FeatureStore client,
*  and contains the instance ID, data source, configuration information, etc. */
public class ApiClient {
    private String instanceId;

    private Configuration configuration;
    Client client;

    private InstanceApi instanceApi;

    private FsProjectApi fsProjectApi;

    private DatasourceApi datasourceApi;

    private FeatureEntityApi featureEntityApi;

    private  FeatureViewApi featureViewApi;

    private  FsModelApi fsModelApi;
    /*  Initialize the construction method  */
    public ApiClient(Configuration configuration) throws Exception {
        this.configuration = configuration;
        this.client = new Client(configuration.getConfig());

        this.instanceApi = new InstanceApi(this);
        this.fsProjectApi = new FsProjectApi(this);
        this.datasourceApi = new DatasourceApi(this);
        this.featureEntityApi = new FeatureEntityApi(this);
        this.featureViewApi = new FeatureViewApi(this);
        this.fsModelApi = new FsModelApi(this);
    }

    public Client getClient() {
        return client;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public InstanceApi getInstanceApi() {
        return instanceApi;
    }

    public FsProjectApi getFsProjectApi() {
        return fsProjectApi;
    }

    public DatasourceApi getDatasourceApi() {
        return datasourceApi;
    }

    public FeatureEntityApi getFeatureEntityApi() {
        return featureEntityApi;
    }

    public FeatureViewApi getFeatureViewApi() {
        return featureViewApi;
    }

    public FsModelApi getFsModelApi() {
        return fsModelApi;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void close() throws Exception {
    }
}
