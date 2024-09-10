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
/*
 * This class contains information about the feature view.*/
public class FeatureViewApi {

    private ApiClient apiClient;

    /*  Initialize the construction method.*/
    public FeatureViewApi(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    public ApiClient getApiClient() {
        return apiClient;
    }

    public void setApiClient(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    /* Gets a collection of feature views based on project id, number of pages, and size of each page
    * @Param projectId(@code String)
    * @Param pageNumber(@code int)
    * @Param pageSize(@code int)
    * @return ListFeatureViewsResponse,The class of ListFeatureViewsResponse the response characteristics of the view.*/
    public ListFeatureViewsResponse listFeatureViews(String projectId, int pageNumber, int pageSize) throws Exception {
        ListFeatureViewsResponse listFeatureViewsResponse = new ListFeatureViewsResponse();
        ListFeatureViewsRequest request = new ListFeatureViewsRequest();
        request.setProjectId(projectId);
        request.setPageSize(pageSize);
        request.setPageNumber(pageNumber);

        com.aliyun.paifeaturestore20230621.models.ListFeatureViewsResponse response = this.apiClient.getClient().listFeatureViews(
                this.apiClient.getInstanceId(), request);

        List<FeatureView> featureViewList = new ArrayList<>();

        listFeatureViewsResponse.setTotalCount(response.getBody().totalCount);
        //  Traverse all characteristic views of the response set.
        for (ListFeatureViewsResponseBody.ListFeatureViewsResponseBodyFeatureViews view: response.getBody().getFeatureViews()) {
            FeatureView featureView = new FeatureView();
            featureView.setFeatureViewId(Long.valueOf(view.getFeatureViewId()));
            featureView.setType(view.getType());
            featureView.setFeatureEntityName(view.getFeatureEntityName());
            featureView.setProjectName(view.getProjectName());
            featureView.setProjectId(Long.valueOf(view.getProjectId()));
            featureView.setWriteToFeaturedb(view.getWriteToFeatureDB());
            featureViewList.add(featureView);
        }
        listFeatureViewsResponse.setFeatureViews(featureViewList);
        return listFeatureViewsResponse;
    }

    /*  Obtain the feature view information based on the feature view id.
    * @Param featureViewId(@code String)
    * @return FeatureView*/
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
        featureView.setWriteToFeaturedb(response.getBody().getWriteToFeatureDB());

        // Check whether the registry information of the current response class exists.
        if (!StringUtils.isEmpty(response.getBody().getRegisterTable())) {
            featureView.setIsRegister(true);
            featureView.setRegisterTable(response.getBody().getRegisterTable());
        }
       // Determines whether the registration data source id of the current response class exists.
        if (!StringUtils.isEmpty(response.getBody().getRegisterDatasourceId())) {
            featureView.setRegisterDatasourceId(Integer.valueOf(response.getBody().getRegisterDatasourceId()));
        }
        //  Check whether the configuration class of the current response class exists.
        if (!StringUtils.isEmpty(response.getBody().getLastSyncConfig())) {
            featureView.setLastSyncConfig(response.getBody().getLastSyncConfig());
        }
        // Creates a collection that holds the feature view request fields.
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
            } else if (f.getType().equals("ARRAY<INT32>")){
                field.setType(FSType.FS_ARRAY_INT32);
            } else if (f.getType().equals("ARRAY<INT64>")) {
                field.setType(FSType.FS_ARRAY_INT64);
            } else if (f.getType().equals("ARRAY<FLOAT>")){
                field.setType(FSType.FS_ARRAY_FLOAT);
            } else if (f.getType().equals("ARRAY<STRING>")){
                field.setType(FSType.FS_ARRAY_STRING);
            } else if (f.getType().equals("ARRAY<DOUBLE>")){
                field.setType(FSType.FS_ARRAY_DOUBLE);
            } else if (f.getType().equals("ARRAY<ARRAY<FLOAT>>")){
                field.setType(FSType.FS_ARRAY_ARRAY_FLOAT);
            } else if (f.getType().equals("MAP<INT32,INT32>")){
                field.setType(FSType.FS_MAP_INT32_INT32);
            } else if (f.getType().equals("MAP<INT32,INT64>")){
                field.setType(FSType.FS_MAP_INT32_INT64);
            } else if (f.getType().equals("MAP<INT32,FLOAT>")){
                field.setType(FSType.FS_MAP_INT32_FLOAT);
            } else if (f.getType().equals("MAP<INT32,DOUBLE>")){
                field.setType(FSType.FS_MAP_INT32_DOUBLE);
            } else if (f.getType().equals("MAP<INT32,STRING>")){
                field.setType(FSType.FS_MAP_INT32_STRING);
            } else if (f.getType().equals("MAP<INT64,INT32>")){
                field.setType(FSType.FS_MAP_INT64_INT32);
            } else if (f.getType().equals("MAP<INT64,INT64>")){
                field.setType(FSType.FS_MAP_INT64_INT64);
            } else if (f.getType().equals("MAP<INT64,FLOAT>")){
                field.setType(FSType.FS_MAP_INT64_FLOAT);
            } else if (f.getType().equals("MAP<INT64,DOUBLE>")){
                field.setType(FSType.FS_MAP_INT64_DOUBLE);
            } else if (f.getType().equals("MAP<INT64,STRING>")){
                field.setType(FSType.FS_MAP_INT64_STRING);
            } else if (f.getType().equals("MAP<STRING,INT32>")){
                field.setType(FSType.FS_MAP_STRING_INT32);
            } else if (f.getType().equals("MAP<STRING,INT64>")){
                field.setType(FSType.FS_MAP_STRING_INT64);
            } else if (f.getType().equals("MAP<STRING,FLOAT>")){
                field.setType(FSType.FS_MAP_STRING_FLOAT);
            } else if (f.getType().equals("MAP<STRING,DOUBLE>")){
                field.setType(FSType.FS_MAP_STRING_DOUBLE);
            } else if (f.getType().equals("MAP<STRING,STRING>")){
                field.setType(FSType.FS_MAP_STRING_STRING);
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
