package com.aliyun.openservices.paifeaturestore.domain;

import com.aliyun.openservices.paifeaturestore.model.Datasource;

public class HologresOnlinestore implements OnlineStore {
    Datasource datasource;

    public Datasource getDatasource() {
        return datasource;
    }

    public void setDatasource(Datasource datasource) {
        this.datasource = datasource;
    }

    @Override
    public String getDatasourceName() {
        return this.datasource.getName();
    }

    @Override
    public String getTableName(FeatureView featureView) {
        return String.format("%s_%s_online", featureView.getProject().getProject().getProjectName(),
                featureView.getFeatureView().getName());
    }
}
