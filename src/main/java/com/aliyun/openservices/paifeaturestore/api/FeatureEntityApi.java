package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.openservices.paifeaturestore.model.FeatureEntity;
import com.aliyun.paifeaturestore20230621.models.ListFeatureEntitiesRequest;
import com.aliyun.paifeaturestore20230621.models.ListFeatureEntitiesResponseBody;

import java.util.ArrayList;
import java.util.List;
/*
* This class contains information about the feature entity.*/
public class FeatureEntityApi {
    private ApiClient apiClient;

    /*  Initialize the construction method  */
    public FeatureEntityApi(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    public ApiClient getApiClient() {
        return apiClient;
    }

    public void setApiClient(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    /*
    * Gets all feature entities for the current project based on the project id
    * @Param projectId(@code String)
    * @return ListFeatureEntitiesResponse,The class of ListFeatureEntitiesResponse the response characteristics of the entity.*/
    public ListFeatureEntitiesResponse listFeatureEntities(String projectId, int pageNumber, int pageSize) throws Exception {
        ListFeatureEntitiesResponse listFeatureEntitiesResponse = new ListFeatureEntitiesResponse();
        ListFeatureEntitiesRequest request = new ListFeatureEntitiesRequest();
        request.setProjectId(projectId);
        request.setPageNumber(pageNumber);
        request.setPageSize(pageSize);

        com.aliyun.paifeaturestore20230621.models.ListFeatureEntitiesResponse response = this.apiClient.getClient().listFeatureEntities(
                this.apiClient.getInstanceId(), request);

        List<FeatureEntity> featureEntityList = new ArrayList<>();
        listFeatureEntitiesResponse.setTotalCount(response.getBody().totalCount);
        // Traverse all characteristic entities of the response set.
        for (ListFeatureEntitiesResponseBody.ListFeatureEntitiesResponseBodyFeatureEntities entity: response.getBody().getFeatureEntities()) {
            FeatureEntity featureEntity = new FeatureEntity();
            featureEntity.setFeatureEntityId(Integer.valueOf(entity.getFeatureEntityId()));
            featureEntity.setFeatureEntityName( entity.getName());
            featureEntity.setFeatureEntityJoinid(entity.getJoinId());
            featureEntity.setProjectName(entity.getProjectName());
            featureEntity.setProjectId(Long.valueOf(entity.getProjectId()));
            featureEntity.setParentFeatureEntityId(Integer.valueOf(entity.getParentFeatureEntityId()));
            featureEntity.setParentFeatureEntityName(entity.getParentFeatureEntityName());
            featureEntity.setParentJoinId(entity.getParentJoinId());

            featureEntityList.add(featureEntity);
        }

        listFeatureEntitiesResponse.setFeatureEntities(featureEntityList);
        return listFeatureEntitiesResponse;
    }
}
