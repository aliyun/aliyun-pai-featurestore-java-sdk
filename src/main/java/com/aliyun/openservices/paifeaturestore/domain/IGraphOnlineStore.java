package com.aliyun.openservices.paifeaturestore.domain;

import com.aliyun.openservices.paifeaturestore.model.Datasource;
import com.aliyun.openservices.paifeaturestore.model.Project;

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
    public String getSequenceTableName(SequenceFeatureView sequenceFeatureView){
        return String.format("%s_fv%d_seq",sequenceFeatureView.featureView.getFeatureEntityName(),sequenceFeatureView.featureView.getFeatureViewId());
    }

    @Override
    public String getSeqOfflineTableName(SequenceFeatureView sequenceFeatureView){
       return this.getSequenceTableName(sequenceFeatureView);
    }

    @Override
    public String getSeqOnlineTableName(SequenceFeatureView sequenceFeatureView) {
        return this.getSequenceTableName(sequenceFeatureView);
    }
}
