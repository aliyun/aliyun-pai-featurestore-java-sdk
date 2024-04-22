package com.aliyun.openservices.paifeaturestore.domain;

import com.aliyun.openservices.paifeaturestore.model.Datasource;
import com.aliyun.openservices.paifeaturestore.model.Project;

public class FeatureDBOnlinestore implements OnlineStore {
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
        return  featureView.getName();
    }
    @Override
    public String getSeqOfflineTableName(SequenceFeatureView sequenceFeatureView){
        return "";
    }

    @Override
    public String getSeqOnlineTableName(SequenceFeatureView sequenceFeatureView) {
        return "";
    }
}
