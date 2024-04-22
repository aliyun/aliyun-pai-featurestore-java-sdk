package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.openservices.paifeaturestore.constants.DatasourceType;
import com.aliyun.openservices.paifeaturestore.model.Datasource;
import com.aliyun.paifeaturestore20230621.models.GetDatasourceResponse;
import com.aliyun.paifeaturestore20230621.models.ListDatasourcesRequest;
import com.aliyun.paifeaturestore20230621.models.ListDatasourcesResponse;
import com.aliyun.paifeaturestore20230621.models.ListDatasourcesResponseBody;
import com.google.gson.Gson;
import org.bouncycastle.util.Strings;

import java.util.Map;

public class DatasourceApi {

    private ApiClient apiClient;

    /*  Initialize the data source  */
    public DatasourceApi(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    public ApiClient getApiClient() {
        return apiClient;
    }

    public void setApiClient(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    /*  Obtain the data source method based on the data source ID
    * @Param datasourceId (@code int)
    * @return Datasource    */
    public Datasource getDatasourceById(int datasourceId) throws Exception {
        Datasource datasource = new Datasource();
        datasource.setDatasourceId(datasourceId);

        GetDatasourceResponse response = apiClient.getClient().getDatasource(apiClient.getInstanceId(), String.valueOf(datasourceId));

        datasource.setName(response.getBody().name);

        if ("Hologres".equals(response.getBody().type)) { //hologres data source
            datasource.setType(DatasourceType.Datasource_Type_Hologres);
            String[] uris = Strings.split(response.getBody().uri, '/');
            datasource.setDatabase(uris[1]);
            datasource.setVpcAddress(String.format("%s-%s-vpc-st.hologres.aliyuncs.com:80", uris[0],
                    apiClient.getConfiguration().getConfig().regionId));
            datasource.setPublicAddress(String.format("%s-%s.hologres.aliyuncs.com:80",
                    uris[0],apiClient.getConfiguration().getConfig().regionId));

        } else if ("GraphCompute".equals(response.getBody().type)) {//GraphCompute data source
            datasource.setType(DatasourceType.Datasource_Type_IGraph);
            Gson gson = new Gson();
            Map map = gson.fromJson(response.getBody().config, Map.class);
            datasource.setVpcAddress(String.valueOf(map.get("address")));
            datasource.setPublicAddress(String.format("%s.public.igraph.aliyuncs.com",response.getBody().uri));
            datasource.setUser(String.valueOf(map.get("username")));
            datasource.setPwd(String.valueOf(map.get("password")));
            datasource.setRdsInstanceId(response.getBody().uri);

        } else if ("Tablestore".equals(response.getBody().type)) { //Tablestore data source
            datasource.setType(DatasourceType.Datasource_Type_TableStore);
            datasource.setRdsInstanceId(response.getBody().uri);
            datasource.setVpcAddress(String.format("https://%s.%s.vpc.tablestore.aliyuncs.com",
                    response.getBody().uri, apiClient.getConfiguration().getConfig().regionId));
            datasource.setPublicAddress(String.format("https://%s.%s.ots.aliyuncs.com",
                    response.getBody().uri,apiClient.getConfiguration().getConfig().regionId));

        } else if ("MaxCompute".equals(response.getBody().type)) {//MC data source
            datasource.setType(DatasourceType.Datasource_Type_MaxCompute);
            datasource.setProject(response.getBody().uri);
            datasource.setWorkspaceId(response.getBody().getWorkspaceId());
        } else if ("FeatureDB".equals(response.getBody().type)) { // featuredb
            datasource.setType(DatasourceType.Datasource_Type_FeatureDB);
            Gson gson = new Gson();
            Map map = gson.fromJson(response.getBody().config, Map.class);
            datasource.setVpcAddress(String.valueOf(map.get("fdb_vpc_address")));
            datasource.setPublicAddress(String.valueOf(map.get("fdb_public_address")));
            datasource.setToken(String.valueOf(map.get("token")));

        }
        return datasource;
    }
    public Datasource getFeatureDBDatasource(String workspaceId) throws Exception {
        ListDatasourcesRequest listDatasourcesRequest = new ListDatasourcesRequest();
        listDatasourcesRequest.setType("FeatureDB");
        listDatasourcesRequest.setWorkspaceId(workspaceId);
        ListDatasourcesResponse listDatasourcesResponse =  apiClient.getClient().listDatasources(apiClient.getInstanceId(), listDatasourcesRequest);

        for (ListDatasourcesResponseBody.ListDatasourcesResponseBodyDatasources body : listDatasourcesResponse.getBody().getDatasources()){
            return this.getDatasourceById(Integer.valueOf(body.getDatasourceId()));
        }
        return null;
    }
}
