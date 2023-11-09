package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.paifeaturestore20230621.models.ListInstancesRequest;
import com.aliyun.paifeaturestore20230621.models.ListInstancesResponse;
import com.aliyun.paifeaturestore20230621.models.ListInstancesResponseBody;
import com.aliyun.tea.utils.StringUtils;

public class InstanceApi {
    private ApiClient apiClient;

    public InstanceApi(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    public ApiClient getApiClient() {
        return apiClient;
    }

    public void setApiClient(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    public void getInstance() throws Exception {
        ListInstancesRequest request = new ListInstancesRequest();
        request.setStatus("Running");

        ListInstancesResponse response = apiClient.getClient().listInstances(request);

        if (response.getBody().getInstances().size() == 0) {
            throw new RuntimeException("not found PAI-FeatureStore running instance");
        }

        String instanceId = null;
        for (ListInstancesResponseBody.ListInstancesResponseBodyInstances instance : response.getBody().getInstances()) {
            if (instance.getRegionId().equals(apiClient.getClient()._regionId)) {
                instanceId = instance.getInstanceId();
                break;
            }
        }

        if (StringUtils.isEmpty(instanceId)) {
            throw new RuntimeException(String.format("region:%s, not found PAI-FeatureStore instance", apiClient.getClient()._regionId));
        }

        apiClient.setInstanceId(instanceId);
    }
}
