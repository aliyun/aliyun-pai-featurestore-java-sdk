package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.openservices.paifeaturestore.constants.DatasourceType;
import com.aliyun.openservices.paifeaturestore.model.Project;
import com.aliyun.paifeaturestore20230621.models.ListProjectsRequest;
import com.aliyun.paifeaturestore20230621.models.ListProjectsResponse;
import com.aliyun.paifeaturestore20230621.models.ListProjectsResponseBody;

import java.util.ArrayList;
import java.util.List;
/*
 * This class contains information about the featurestore project.*/
public class FsProjectApi {
    private ApiClient apiClient;

    public FsProjectApi(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    public ApiClient getApiClient() {
        return apiClient;
    }

    public void setApiClient(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    /*  Get a list of items
    * @return ListProjectResponse,this class contains all the items currently under FeatureStore.*/
    public ListProjectResponse ListProjects() throws Exception {
        ListProjectsRequest request = new ListProjectsRequest();
        request.setName(apiClient.getConfiguration().getProjectName());
        ListProjectsResponse response = apiClient.getClient().listProjects(apiClient.getInstanceId(), request);
        List<Project> projects = new ArrayList<>();
        ListProjectResponse listProjectResponse = new ListProjectResponse();

        for (ListProjectsResponseBody.ListProjectsResponseBodyProjects projectItem : response.getBody().getProjects()) {
            Project project = new Project();
            project.setProjectId(Long.valueOf(projectItem.projectId));
            project.setProjectName(projectItem.name);
            project.setOfflineDatasourceId(Integer.valueOf(projectItem.offlineDatasourceId));
            project.setOnlineDatasourceId(Integer.valueOf(projectItem.onlineDatasourceId));
            if ("MaxCompute".equals(projectItem.offlineDatasourceType)) {
               project.setOfflineDatasourceType(DatasourceType.Datasource_Type_MaxCompute);
            }

            if ("Hologres".equals(projectItem.onlineDatasourceType)) {//Hologres类型
                project.setOnlineDatasourceType(DatasourceType.Datasource_Type_Hologres);
            } else if ("GraphCompute".equals(projectItem.onlineDatasourceType)) {//GraphCompute类型
                project.setOnlineDatasourceType(DatasourceType.Datasource_Type_IGraph);
            } else if ("Tablestore".equals(projectItem.onlineDatasourceType)) {//Tablestore类型
                project.setOnlineDatasourceType(DatasourceType.Datasource_Type_TableStore);
            }
            projects.add(project);
        }

        listProjectResponse.setProjects(projects);

        return listProjectResponse;
    }
}
