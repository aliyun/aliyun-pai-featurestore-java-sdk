package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.aliyun.openservices.paifeaturestore.model.FeatureView;
import com.aliyun.openservices.paifeaturestore.model.FeatureViewRequestFields;
import com.aliyun.paifeaturestore20230621.models.GetFeatureViewResponse;
import com.aliyun.paifeaturestore20230621.models.GetFeatureViewResponseBody;
import com.aliyun.paifeaturestore20230621.models.ListFeatureViewsRequest;
import com.aliyun.paifeaturestore20230621.models.ListFeatureViewsResponseBody;
import com.aliyun.tea.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class FeatureViewApi {
    private ApiClient apiClient;

    public FeatureViewApi(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    public ApiClient getApiClient() {
        return apiClient;
    }

    public void setApiClient(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    public ListFeatureViewsResponse listFeatureViews(String projectId, int pageNumber, int pageSize) throws Exception {
        ListFeatureViewsResponse listFeatureViewsResponse = new ListFeatureViewsResponse();

        ListFeatureViewsRequest request = new ListFeatureViewsRequest();
        request.setProjectId(projectId);
        request.setPageSize(pageSize);
        request.setPageNumber(pageNumber);
        com.aliyun.paifeaturestore20230621.models.ListFeatureViewsResponse response = this.apiClient.getClient().listFeatureViews(this.apiClient.getInstanceId(), request);

        List<FeatureView> featureViewList = new ArrayList<>();

        listFeatureViewsResponse.setTotalCount(response.getBody().totalCount);
        for (ListFeatureViewsResponseBody.ListFeatureViewsResponseBodyFeatureViews view: response.getBody().getFeatureViews()) {
            FeatureView featureView = new FeatureView();
            featureView.setFeatureViewId(Long.valueOf(view.getFeatureViewId()));
            featureView.setType(view.getType());
            featureView.setFeatureEntityName(view.getFeatureEntityName());
            featureView.setProjectName(view.getProjectName());
            featureView.setProjectId(Long.valueOf(view.getProjectId()));

            featureViewList.add(featureView);
        }

        listFeatureViewsResponse.setFeatureViews(featureViewList);
        return listFeatureViewsResponse;
    }

    public FeatureView getFeatureViewById(String featureViewId) throws Exception {
        GetFeatureViewResponse response = this.apiClient.getClient().getFeatureView(this.apiClient.getInstanceId(), featureViewId);
        FeatureView featureView = new FeatureView();
        featureView.setFeatureViewId(Long.valueOf(featureViewId));
        featureView.setProjectId(Long.valueOf(response.getBody().getProjectId()));
        featureView.setProjectName(response.getBody().getProjectName());
        featureView.setName(response.getBody().getName());
        featureView.setType(response.getBody().getType());
        featureView.setConfig(response.getBody().getConfig());
        featureView.setTtl(response.getBody().getTTL());
        featureView.setOnline(response.getBody().getSyncOnlineTable());
        featureView.setFeatureEntityId(Integer.valueOf(response.getBody().getFeatureEntityId()));
        featureView.setFeatureEntityName(response.getBody().getFeatureEntityName());

        if (!StringUtils.isEmpty(response.getBody().getRegisterTable())) {
            featureView.setIsRegister(true);
            featureView.setRegisterTable(response.getBody().getRegisterTable());
        }

        if (!StringUtils.isEmpty(response.getBody().getRegisterDatasourceId())) {
            featureView.setRegisterDatasourceId(Integer.valueOf(response.getBody().getRegisterDatasourceId()));
        }

        if (!StringUtils.isEmpty(response.getBody().getLastSyncConfig())) {
            featureView.setLastSyncConfig(response.getBody().getLastSyncConfig());
        }

        List<FeatureViewRequestFields> fields = new ArrayList<>();
        int pos = 0;
        for (GetFeatureViewResponseBody.GetFeatureViewResponseBodyFields f : response.getBody().getFields()) {
            FeatureViewRequestFields field = new FeatureViewRequestFields();
            field.setPosition(++pos);
            field.setName( f.getName());
            if (f.getType().equals("STRING")) {
                field.setType(FSType.FS_STRING);
            } else if (f.getType().equals("INT32")) {
                field.setType(FSType.FS_INT32);
            } else if (f.getType().equals("INT64")) {
                field.setType(FSType.FS_INT64);
            } else if (f.getType().equals("FLOAT")) {
                field.setType(FSType.FS_FLOAT);
            } else if (f.getType().equals("DOUBLE")) {
                field.setType(FSType.FS_DOUBLE);
            } else if (f.getType().equals("BOOLEAN")) {
                field.setType(FSType.FS_BOOLEAN);
            } else if (f.getType().equals("TIMESTAMP")) {
                field.setType(FSType.FS_TIMESTAMP);
            }

            if (null != f.getAttributes()) {
                for (String attr : f.getAttributes()) {
                    if (attr.equals("Partition")) {
                        field.setIsPartition(true);
                    }

                    if (attr.equals("PrimaryKey")) {
                        field.setIsPrimaryKey(true);
                    }

                    if (attr.equals("EventTime")) {
                        field.setIsEventTime(true);
                    }
                }

            }

            fields.add(field);
        }

        featureView.setFields(fields);

        return featureView;
    }
}
