package com.aliyun.openservices.paifeaturestore;

import com.aliyun.openservices.paifeaturestore.api.ApiClient;
import com.aliyun.openservices.paifeaturestore.api.ListProjectResponse;
import com.aliyun.openservices.paifeaturestore.domain.Project;
import com.aliyun.openservices.paifeaturestore.model.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
/* This class is a yes operation. FeatureStore is a FS client to be configured.*/
public class FeatureStoreClient {

    private ApiClient apiClient;

    private Map<String, Project> projects = new ConcurrentHashMap<>();

    public FeatureStoreClient(ApiClient apiClient, boolean usePublicAddress ) throws Exception {
        this.apiClient = apiClient;
        this.apiClient.getInstanceApi().getInstance();
        this.loadProjectData(usePublicAddress);
    }

    public FeatureStoreClient(ApiClient apiClient ) throws Exception {
        this(apiClient, false);
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

            // featureView / featureEntity / model 改为按需懒加载（见 domain.Project），
            // 这里只注入 apiClient，不再全量拉取
            domainProject.setApiClient(this.apiClient);

            projectMap.put(project.getProjectName(), domainProject);
        }

        if (projectMap.size() > 0) {
            this.projects = projectMap;
        }
    }

    public Project getProject(String name) {
        return this.projects.get(name);
    }
}
