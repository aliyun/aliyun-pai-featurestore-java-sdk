package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.aliyun.openservices.paifeaturestore.model.Model;
import com.aliyun.openservices.paifeaturestore.model.ModelFeatures;
import com.aliyun.paifeaturestore20230621.models.GetModelFeatureResponse;
import com.aliyun.paifeaturestore20230621.models.GetModelFeatureResponseBody;
import com.aliyun.paifeaturestore20230621.models.ListModelFeaturesRequest;
import com.aliyun.paifeaturestore20230621.models.ListModelFeaturesResponseBody;
import com.aliyun.tea.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;
/*
 * This class contains information about the feature model.*/
public class FsModelApi {

    private ApiClient apiClient;

    /*  Initialize the construction method. */
    public FsModelApi(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    public ApiClient getApiClient() {
        return apiClient;
    }

    public void setApiClient(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    /*  Get the model based on the model id.
    * @Param modelId(@code String)
    * @return Model*/
    public Model getModelById(String modelId) throws Exception {
        GetModelFeatureResponse response = this.apiClient.getClient().getModelFeature(this.apiClient.getInstanceId(), modelId);
        Model model = new Model();
        model.setModelId(Long.valueOf(modelId));
        model.setName(response.getBody().getName());
        model.setProjectName(response.getBody().getProjectName());
        model.setProjectId(Long.valueOf(response.getBody().getProjectId()));

        List<ModelFeatures> features = new ArrayList<>();
        //  Traverse the feature set of the model feature response class to get the current field.
        for (GetModelFeatureResponseBody.GetModelFeatureResponseBodyFeatures field : response.getBody().getFeatures()) {
            ModelFeatures feature = new ModelFeatures();
            feature.setFeatureViewName(field.getFeatureViewName());
            feature.setName(field.getName());
            feature.setFeatureViewId(Integer.valueOf(field.getFeatureViewId()));
            //  Determines whether the field of the current model feature is empty and aliased.
            if (!StringUtils.isEmpty(field.getAliasName()) && !field.getAliasName().equals(field.getName())) {
                feature.setAliasName(field.getAliasName());
            }
            //  Determine the type of the current field and set the corresponding type.
            if (field.getType().equals("STRING")) {
                feature.setType(FSType.FS_STRING);
            } else if (field.getType().equals("INT32")) {
                feature.setType(FSType.FS_INT32);
            } else if (field.getType().equals("INT64")) {
                feature.setType(FSType.FS_INT64);
            } else if (field.getType().equals("FLOAT")) {
                feature.setType(FSType.FS_FLOAT);
            } else if (field.getType().equals("DOUBLE")) {
                feature.setType(FSType.FS_DOUBLE);
            } else if (field.getType().equals("BOOLEAN")) {
                feature.setType(FSType.FS_BOOLEAN);
            } else if (field.getType().equals("TIMESTAMP")) {
                feature.setType(FSType.FS_TIMESTAMP);
            }

            features.add(feature);
        }
        model.setFeatures(features);

        return model;
    }

    /*
    * Get the model feature set according to project id, number of pages, and size of each page.
    * @Param projectId(@code String)
    * @Param pageNumber(@code int)
    * @Param pageSize(@code int)*/
    public ListModesResponse listModels(String projectId, int pageNumber, int pageSize) throws Exception {
        ListModesResponse listModesResponse = new ListModesResponse();
        ListModelFeaturesRequest request = new ListModelFeaturesRequest();
        request.setProjectId(projectId);
        request.setPageSize(pageSize);
        request.setPageNumber(pageNumber);

        com.aliyun.paifeaturestore20230621.models.ListModelFeaturesResponse response = this.apiClient.getClient().listModelFeatures(
                this.apiClient.getInstanceId(), request);

        List<Model> modelList = new ArrayList<>();

        listModesResponse.setTotalCount(response.getBody().getTotalCount());
        //  The model features are obtained by traversing the response body.
        for (ListModelFeaturesResponseBody.ListModelFeaturesResponseBodyModelFeatures modelFeature : response.getBody().getModelFeatures()) {
            Model model = new Model();
            model.setModelId(Long.valueOf(modelFeature.getModelFeatureId()));
            model.setName(modelFeature.getName());
            model.setProjectId(Long.valueOf(modelFeature.getProjectId()));
            model.setProjectName(modelFeature.getProjectName());
            modelList.add(model);
        }

        listModesResponse.setModels(modelList);
        return listModesResponse;
    }
}
