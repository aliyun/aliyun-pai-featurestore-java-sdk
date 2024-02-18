package com.aliyun.openservices.paifeaturestore.domain;

import com.aliyun.openservices.paifeaturestore.model.FeatureView;

import java.util.Map;

public interface IFeatureView {

    FeatureResult getOnlineFeatures(String[] joinIds) throws Exception;

    FeatureResult getOnlineFeatures(String[] joinIds,String[] features, Map<String, String> aliasFields) throws Exception;

    String getName();

    String getFeatureEntityName();

    String getType();

    FeatureView getFeatureView();

    FeatureEntity getFeatureEntity();
}
