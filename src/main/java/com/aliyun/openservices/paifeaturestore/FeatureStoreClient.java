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
            // apiClient 构造注入，供 Project 懒加载 featureView/entity/model 元数据使用
            Project domainProject = new Project(project, usePublicAddress, this.apiClient);

            domainProject.setUsePublicAddress(usePublicAddress);

            Datasource featureDBDataSource = this.apiClient.getDatasourceApi().getFeatureDBDatasource(offlineDatasource.getWorkspaceId());
            if (featureDBDataSource != null) {
                domainProject.registerFeatrueDB(featureDBDataSource);
            }

            projectMap.put(project.getProjectName(), domainProject);
        }

        if (projectMap.size() > 0) {
            // 拷贝进 ConcurrentHashMap，而不是把字段整体换成普通 HashMap
            this.projects = new ConcurrentHashMap<>(projectMap);
        }
    }

    public Project getProject(String name) {
        return this.projects.get(name);
    }
}
