package com.aliyun.openservices.paifeaturestore.domain;

public interface OnlineStore {
    String getDatasourceName();

    String getTableName(FeatureView featureView);

    String getSeqOfflineTableName(SequenceFeatureView sequenceFeatureView);

    String getSeqOnlineTableName(SequenceFeatureView sequenceFeatureView);
}
