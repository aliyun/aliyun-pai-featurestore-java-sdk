package com.aliyun.openservices.paifeaturestore;

import com.aliyun.openservices.paifeaturestore.api.ApiClient;
import com.aliyun.openservices.paifeaturestore.api.ListProjectResponse;
import com.aliyun.openservices.paifeaturestore.datasource.FeatureDBFactory;
import com.aliyun.openservices.paifeaturestore.datasource.HologresFactory;
import com.aliyun.openservices.paifeaturestore.domain.Project;
import com.aliyun.openservices.paifeaturestore.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/* This class is a yes operation. FeatureStore is a FS client to be configured.*/
public class FeatureStoreClient {
    public static Logger logger = LoggerFactory.getLogger(FeatureStoreClient.class);

    private ApiClient apiClient;

    private Map<String, Project> projects = new ConcurrentHashMap<>();

    private volatile boolean loopData = true;
    ScheduledExecutorService scheduledThreadPool = null;

    public FeatureStoreClient(ApiClient apiClient, boolean usePublicAddress, boolean loopData ) throws Exception {
        this.apiClient = apiClient;
        this.apiClient.getInstanceApi().getInstance();
        this.loadProjectData(usePublicAddress);
        if (loopData) {
            this.scheduledThreadPool = Executors.newScheduledThreadPool(1, new ThreadFactory() {
                private final String namePrefix = "FeatureStore-Reloader-";
                private int threadNum = 1;
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, namePrefix + threadNum++);
                    t.setDaemon(true);
                    return t;
                }
            });
            LoadProjectWorker worker = new LoadProjectWorker(this, usePublicAddress);
            scheduledThreadPool.scheduleWithFixedDelay(worker, 60, 60, TimeUnit.SECONDS);
        }
        this.loopData = loopData;
    }
    public FeatureStoreClient(ApiClient apiClient, boolean usePublicAddress ) throws Exception {
        this(apiClient, usePublicAddress, true);
    }

    public FeatureStoreClient(ApiClient apiClient ) throws Exception {
        this(apiClient, false, true);
    }

    private static class LoadProjectWorker implements Runnable {

        FeatureStoreClient featureStoreClient;
        boolean usePublicAddress ;

        public LoadProjectWorker(FeatureStoreClient client, boolean usePublicAddress) {
            this.featureStoreClient = client;
            this.usePublicAddress = usePublicAddress;
        }

        @Override
        public void run() {
            try {
                this.featureStoreClient.loadProjectData(usePublicAddress);
            } catch (InterruptedException e) {
                logger.info("LoadProjectWorker was interrupted, shutting down.");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.error("load project data failed with an unexpected error", e);
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
                this.projects.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }
    }

    public Project getProject(String name) {
        return this.projects.get(name);
    }

    public void close() throws Exception {
        if (this.loopData && null != this.scheduledThreadPool) {
            this.scheduledThreadPool.shutdownNow();
        }

        for (Project project : this.projects.values()) {
            project.close();
        }
        this.projects.clear();
        //FeatureDBFactory.close();
        HologresFactory.close();
    }
}
