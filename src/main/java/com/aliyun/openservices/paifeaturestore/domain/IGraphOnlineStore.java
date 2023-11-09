package com.aliyun.openservices.paifeaturestore.domain;

import com.aliyun.openservices.paifeaturestore.model.Datasource;

public class IGraphOnlineStore implements OnlineStore {
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
        return String.format("%s_fv%d", featureView.getFeatureView().getFeatureEntityName(),
                featureView.getFeatureView().getFeatureViewId());
    }
}
