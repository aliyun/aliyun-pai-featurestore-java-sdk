package com.aliyun.openservices.paifeaturestore.domain;

public interface OnlineStore {
    String getDatasourceName();

    String getTableName(FeatureView featureView);
}
