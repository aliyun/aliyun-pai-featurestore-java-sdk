package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.openservices.paifeaturestore.model.FeatureEntity;

import java.util.List;

public class ListFeatureEntitiesResponse {
    List<FeatureEntity> featureEntities;

    public List<FeatureEntity> getFeatureEntities() {
        return featureEntities;
    }

    public void setFeatureEntities(List<FeatureEntity> featureEntities) {
        this.featureEntities = featureEntities;
    }
}
