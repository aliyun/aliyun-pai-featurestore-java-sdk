package com.aliyun.openservices.paifeaturestore.domain;

import com.aliyun.openservices.paifeaturestore.constants.FSType;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public interface FeatureResult {
    FSType getType(String featureName);

    boolean isNull(String featureName);

    Object getObject(String featureName);

    String getString(String featureName);

    int getInt(String featureName);

    float getFloat(String featureName);

    double getDouble(String featureName);

    long getLong(String featureName);

    boolean getBoolean(String featureName);

    Timestamp getTimestamp(String featureName);

    boolean next();

    List<Map<String, Object>> getFeatureData();

    String[] getFeatureFields();

    Map<String, FSType> getFeatureFieldTypeMap();

}
