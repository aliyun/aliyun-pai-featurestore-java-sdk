package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.openservices.paifeaturestore.model.FeatureEntity;
import com.aliyun.paifeaturestore20230621.models.ListFeatureEntitiesRequest;
import com.aliyun.paifeaturestore20230621.models.ListFeatureEntitiesResponseBody;

import java.util.ArrayList;
import java.util.List;

public class FeatureEntityApi {
    private ApiClient apiClient;

    public FeatureEntityApi(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    public ApiClient getApiClient() {
        return apiClient;
    }

    public void setApiClient(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    public ListFeatureEntitiesResponse listFeatureEntities(String projectId) throws Exception {
        ListFeatureEntitiesResponse listFeatureEntitiesResponse = new ListFeatureEntitiesResponse();

        ListFeatureEntitiesRequest request = new ListFeatureEntitiesRequest();
        request.setProjectId(projectId);
        request.setPageSize(100);
        com.aliyun.paifeaturestore20230621.models.ListFeatureEntitiesResponse response = this.apiClient.getClient().listFeatureEntities(this.apiClient.getInstanceId(), request);

        List<FeatureEntity> featureEntityList = new ArrayList<>();

        for (ListFeatureEntitiesResponseBody.ListFeatureEntitiesResponseBodyFeatureEntities entity: response.getBody().getFeatureEntities()) {
            FeatureEntity featureEntity = new FeatureEntity();
            featureEntity.setFeatureEntityId(Integer.valueOf(entity.getFeatureEntityId()));
            featureEntity.setFeatureEntityName( entity.getName());
            featureEntity.setFeatureEntityJoinid(entity.getJoinId());
            featureEntity.setProjectName(entity.getProjectName());
            featureEntity.setProjectId(Long.valueOf(entity.getProjectId()));

            featureEntityList.add(featureEntity);
        }

        listFeatureEntitiesResponse.setFeatureEntities(featureEntityList);
        return listFeatureEntitiesResponse;
    }
}
