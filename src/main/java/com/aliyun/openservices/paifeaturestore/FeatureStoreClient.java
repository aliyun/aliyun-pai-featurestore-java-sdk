package com.aliyun.openservices.paifeaturestore;

import com.aliyun.openservices.paifeaturestore.api.ApiClient;
import com.aliyun.openservices.paifeaturestore.api.ListFeatureEntitiesResponse;
import com.aliyun.openservices.paifeaturestore.api.ListFeatureViewsResponse;
import com.aliyun.openservices.paifeaturestore.api.ListModesResponse;
import com.aliyun.openservices.paifeaturestore.api.ListProjectResponse;
import com.aliyun.openservices.paifeaturestore.domain.FeatureViewFactory;
import com.aliyun.openservices.paifeaturestore.domain.IFeatureView;
import com.aliyun.openservices.paifeaturestore.domain.Project;
import com.aliyun.openservices.paifeaturestore.domain.SequenceFeatureView;
import com.aliyun.openservices.paifeaturestore.model.*;

import java.util.HashMap;
import java.util.Map;
/* This class is a yes operation. FeatureStore is a FS client to be configured.*/
public class FeatureStoreClient {

    private ApiClient apiClient;

    private Map<String, Project> projects = new HashMap<>();

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

            Project domainProject = new Project(project,usePublicAddress);

            projectMap.put(project.getProjectName(), domainProject);

            ListFeatureEntitiesResponse listFeatureEntitiesResponse = this.apiClient.getFeatureEntityApi().listFeatureEntities(String.valueOf(project.getProjectId()));

            for(FeatureEntity featureEntity : listFeatureEntitiesResponse.getFeatureEntities()) {
                if (featureEntity.getProjectId() == project.getProjectId()) {
                    domainProject.addFeatureEntity( featureEntity.getFeatureEntityName(), new com.aliyun.openservices.paifeaturestore.domain.FeatureEntity(featureEntity));
                }
            }

            int pageNumber = 1;
            int pageSize = 100;
            do {

                ListFeatureViewsResponse listFeatureViewsResponse =  this.apiClient.getFeatureViewApi().listFeatureViews(String.valueOf(project.getProjectId()), pageNumber, pageSize);
                for (FeatureView view: listFeatureViewsResponse.getFeatureViews()) {

                    FeatureView featureView = this.apiClient.getFeatureViewApi().getFeatureViewById(String.valueOf(view.getFeatureViewId()));
                    if (featureView.getRegisterDatasourceId() > 0) {
                        Datasource registerDatasource = this.apiClient.getDatasourceApi().getDatasourceById(featureView.getRegisterDatasourceId());
                        featureView.setRegisterDatasource(registerDatasource);
                    }

                    IFeatureView domainFeatureView = FeatureViewFactory.getFeatureView(featureView, domainProject, domainProject.getFeatureEntityMap().get(featureView.getFeatureEntityName()) );

                    domainProject.addFeatureView(featureView.getName(), domainFeatureView);

                }


                if (listFeatureViewsResponse.getFeatureViews().size() == 0 || pageNumber * pageSize > listFeatureViewsResponse.getTotalCount()) {
                    break;
                }

                pageNumber++;
            } while (true);

            pageNumber = 1;
            do {
                ListModesResponse listModesResponse = this.apiClient.getFsModelApi().listModels(String.valueOf(project.getProjectId()), pageNumber, pageSize);
                for (Model m : listModesResponse.getModels()) {
                    Model model = this.apiClient.getFsModelApi().getModelById(String.valueOf(m.getModelId()));
                    com.aliyun.openservices.paifeaturestore.domain.Model domianModel = new com.aliyun.openservices.paifeaturestore.domain.Model(model, domainProject);

                    domainProject.addModel(model.getName(), domianModel);
                }
                if (listModesResponse.getModels().size() == 0 || pageNumber * pageSize > listModesResponse.getTotalCount()) {
                    break;
                }
                pageNumber++;
            } while (true);
        }

        if (projectMap.size() > 0) {
            this.projects = projectMap;
        }
    }

    public Project getProject(String name) {
        return this.projects.get(name);
    }
}
