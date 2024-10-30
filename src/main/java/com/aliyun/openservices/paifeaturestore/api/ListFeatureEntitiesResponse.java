package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.openservices.paifeaturestore.model.FeatureEntity;

import java.util.List;
/*
 * This class contains list information composed of FeatureEntities.*/
public class ListFeatureEntitiesResponse {
    List<FeatureEntity> featureEntities;
    private Integer totalCount;

    public List<FeatureEntity> getFeatureEntities() {
        return featureEntities;
    }

    public void setFeatureEntities(List<FeatureEntity> featureEntities) {
        this.featureEntities = featureEntities;
    }

    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;
    }

    public Integer getTotalCount() {
        return totalCount;
    }
}
