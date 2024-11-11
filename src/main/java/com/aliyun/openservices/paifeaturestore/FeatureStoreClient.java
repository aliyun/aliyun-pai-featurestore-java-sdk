package com.aliyun.openservices.paifeaturestore;

import com.aliyun.openservices.paifeaturestore.api.ApiClient;
import com.aliyun.openservices.paifeaturestore.api.ListProjectResponse;
import com.aliyun.openservices.paifeaturestore.domain.Project;
import com.aliyun.openservices.paifeaturestore.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/* This class is a yes operation. FeatureStore is a FS client to be configured.*/
public class FeatureStoreClient {
    public static Logger logger = LoggerFactory.getLogger(FeatureStoreClient.class);

    private ApiClient apiClient;

    private Map<String, Project> projects = new HashMap<>();

    ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(2);

    public FeatureStoreClient(ApiClient apiClient, boolean usePublicAddress ) throws Exception {
        this.apiClient = apiClient;
        this.apiClient.getInstanceApi().getInstance();
        this.loadProjectData(usePublicAddress);
        LoadProjectWorker worker = new LoadProjectWorker(this, usePublicAddress);
        scheduledThreadPool.scheduleWithFixedDelay(worker, 60, 60, TimeUnit.SECONDS);
    }

    public FeatureStoreClient(ApiClient apiClient ) throws Exception {
        this(apiClient, false);
    }

    private static class LoadProjectWorker extends Thread {

        FeatureStoreClient featureStoreClient;
        boolean usePublicAddress ;

        public LoadProjectWorker(FeatureStoreClient client, boolean usePublicAddress) {
            this.featureStoreClient = client;
            this.usePublicAddress = usePublicAddress;
            super.setDaemon(true);
        }

        @Override
        public void run() {
            try {
                this.featureStoreClient.loadProjectData(usePublicAddress);
            } catch (Exception e) {
                logger.error("load project error", e);
            }
        }
    }

    /**
     * load data from fs server
     * @throws Exception
     */
    private void loadProjectData(boolean usePublicAddress) throws Exception {
        AK ak = new AK();
        ak.setAccessId(this.apiClient.getConfiguration().getConfig().getAccessKeyId());
        ak.setAccessKey(this.apiClient.getConfiguration().getConfig().getAccessKeySecret());

        Map<String, Project> projectMap = new HashMap<>();

        ListProjectResponse listProjectResponse = this.apiClient.getFsProjectApi().ListProjects();

        for( com.aliyun.openservices.paifeaturestore.model.Project project : listProjectResponse.getProjects()) {

            Datasource datasource = this.apiClient.getDatasourceApi().getDatasourceById(project.getOnlineDatasourceId());
            datasource.setAk(ak);
            project.setOnlineDataSource(datasource);

            Datasource offlineDatasource = this.apiClient.getDatasourceApi().getDatasourceById(project.getOfflineDatasourceId());
            offlineDatasource.setAk(ak);
            project.setOfflineDataSource(offlineDatasource);

            project.createSignature(this.apiClient.getConfiguration().getUsername(), this.apiClient.getConfiguration().getPassword());
            Project domainProject = new Project(project,usePublicAddress);

            domainProject.setUsePublicAddress(usePublicAddress);

            Datasource featureDBDataSource = this.apiClient.getDatasourceApi().getFeatureDBDatasource(offlineDatasource.getWorkspaceId());
            if (featureDBDataSource != null) {
                domainProject.registerFeatrueDB(featureDBDataSource);
            }

            domainProject.setApiClient(this.apiClient);
            projectMap.put(project.getProjectName(), domainProject);

        }

        if (projectMap.size() > 0) {
            for (Map.Entry<String, Project> entry : projectMap.entrySet()) {
                if (!this.projects.containsKey(entry.getKey())) {
                    this.projects.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    public Project getProject(String name) {
        return this.projects.get(name);
    }
}
