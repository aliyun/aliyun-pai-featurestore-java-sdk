package com.aliyun.openservices.paifeaturestore.domain;

import com.aliyun.openservices.paifeaturestore.constants.InsertMode;
import com.aliyun.openservices.paifeaturestore.model.FeatureView;

import java.util.List;
import java.util.Map;

public interface IFeatureView {

    FeatureResult getOnlineFeatures(String[] joinIds) throws Exception;

    FeatureResult getOnlineFeatures(String[] joinIds,String[] features, Map<String, String> aliasFields) throws Exception;

    String getName();

    String getFeatureEntityName();

    String getType();

    FeatureView getFeatureView();

    FeatureEntity getFeatureEntity();

    void writeFeatures(List<Map<String, Object>> data);
    void writeFeatures(List<Map<String, Object>> data, InsertMode insertMode);
}
