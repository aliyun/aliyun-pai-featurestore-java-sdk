package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.openservices.paifeaturestore.constants.DatasourceType;
import com.aliyun.openservices.paifeaturestore.model.Datasource;
import com.aliyun.paifeaturestore20230621.models.GetDatasourceResponse;
import com.google.gson.Gson;
import org.bouncycastle.util.Strings;

import java.util.Map;

public class DatasourceApi {
    private ApiClient apiClient;

    public DatasourceApi(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    public ApiClient getApiClient() {
        return apiClient;
    }

    public void setApiClient(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    public Datasource getDatasourceById(int datasourceId) throws Exception {
        Datasource datasource = new Datasource();
        datasource.setDatasourceId(datasourceId);
        GetDatasourceResponse response = apiClient.getClient().getDatasource(apiClient.getInstanceId(), String.valueOf(datasourceId));

        datasource.setName(response.getBody().name);

        if ("Hologres".equals(response.getBody().type)) {
            datasource.setType(DatasourceType.Datasource_Type_Hologres);
            String[] uris = Strings.split(response.getBody().uri, '/');
            datasource.setDatabase(uris[1]);
            datasource.setVpcAddress(String.format("%s-%s-vpc-st.hologres.aliyuncs.com:80", uris[0], apiClient.getConfiguration().getConfig().regionId));
        } else if ("GraphCompute".equals(response.getBody().type)) {
            datasource.setType(DatasourceType.Datasource_Type_IGraph);
            Gson gson = new Gson();
            Map map = gson.fromJson(response.getBody().config, Map.class);
            datasource.setVpcAddress(String.valueOf(map.get("address")));
            datasource.setUser(String.valueOf(map.get("username")));
            datasource.setPwd(String.valueOf(map.get("password")));
            datasource.setRdsInstanceId(response.getBody().uri);
        } else if ("Tablestore".equals(response.getBody().type)) {
            datasource.setType(DatasourceType.Datasource_Type_TableStore);
            datasource.setRdsInstanceId(response.getBody().uri);
            datasource.setVpcAddress(String.format("https://%s.%s.vpc.tablestore.aliyuncs.com", response.getBody().uri, apiClient.getConfiguration().getConfig().regionId));
        } else if ("MaxCompute".equals(response.getBody().type)) {
            datasource.setType(DatasourceType.Datasource_Type_MaxCompute);
            datasource.setProject(response.getBody().uri);
        }
        return datasource;
    }
}
