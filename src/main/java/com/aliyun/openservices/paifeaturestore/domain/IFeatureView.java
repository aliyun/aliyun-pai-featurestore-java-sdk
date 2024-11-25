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

    // write features to featurestore online table, only support featuredb
    void writeFeatures(List<Map<String, Object>> data);
    void writeFeatures(List<Map<String, Object>> data, InsertMode insertMode);
    // flush all data to featurestore, use writeFeatures to write data, when finish only call once
    void writeFlush();

    default void close() throws Exception {

    }
}
