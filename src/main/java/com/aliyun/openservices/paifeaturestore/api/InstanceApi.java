package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.paifeaturestore20230621.models.ListInstancesRequest;
import com.aliyun.paifeaturestore20230621.models.ListInstancesResponse;
import com.aliyun.paifeaturestore20230621.models.ListInstancesResponseBody;
import com.aliyun.tea.utils.StringUtils;
/*
 * This class contains information about the Instance.*/
public class InstanceApi {
    private ApiClient apiClient;

    public InstanceApi(ApiClient apiClient) {//初始化实例
        this.apiClient = apiClient;
    }

    public ApiClient getApiClient() {//获取api客户端
        return apiClient;
    }

    public void setApiClient(ApiClient apiClient) {//设置api客户端
        this.apiClient = apiClient;
    }

    /* Get the instance method*/
    public void getInstance() throws Exception {
        ListInstancesRequest request = new ListInstancesRequest();
        request.setStatus("Running");

        ListInstancesResponse response = apiClient.getClient().listInstances(request);

        if (response.getBody().getInstances().size() == 0) {
            throw new RuntimeException("not found PAI-FeatureStore running instance");
        }

        String instanceId = null;
        //  Traverse the response body
        for (ListInstancesResponseBody.ListInstancesResponseBodyInstances instance : response.getBody().getInstances()) {
            instanceId = instance.getInstanceId();
            break;
        }
        // Determine whether there is an instance of the current id on FS.
        if (StringUtils.isEmpty(instanceId)) {
            throw new RuntimeException(String.format("region:%s, not found PAI-FeatureStore instance", apiClient.getClient()._regionId));
        }

        apiClient.setInstanceId(instanceId);
    }
}
