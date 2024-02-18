package com.aliyun.openservices.paifeaturestore.domain;

import com.aliyun.openservices.paifeaturestore.model.Datasource;
import com.aliyun.openservices.paifeaturestore.model.Project;

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
    @Override
    public String getSeqOfflineTableName(SequenceFeatureView sequenceFeatureView){
        com.aliyun.openservices.paifeaturestore.model.Project p  =sequenceFeatureView.getProject().getProject();
        return String.format("%s_%s_seq_offline",p.getProjectName(),sequenceFeatureView.getFeatureView().getName());
    }

    @Override
    public String getSeqOnlineTableName(SequenceFeatureView sequenceFeatureView) {
        Project p  =sequenceFeatureView.getProject().getProject();
        return String.format("%s_%s_seq",p.getProjectName(),sequenceFeatureView.getFeatureView().getName());
    }
}
