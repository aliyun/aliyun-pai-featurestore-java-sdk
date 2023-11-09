package com.aliyun.openservices.paifeaturestore.dao;

import com.aliyun.openservices.paifeaturestore.domain.FeatureResult;

public interface FeatureViewDao {
    FeatureResult getFeatures(String[] keys, String[] selectFields );
}
