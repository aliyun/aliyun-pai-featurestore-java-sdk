package com.aliyun.openservices.paifeaturestore.api;


import com.aliyun.openservices.paifeaturestore.model.FeatureView;

import java.util.List;
/*
 * This class contains list information composed of FeatureViews.*/
public class ListFeatureViewsResponse {
    Long totalCount ;
    List<FeatureView> featureViews;

    public List<FeatureView> getFeatureViews() {
        return featureViews;
    }

    public void setFeatureViews(List<FeatureView> featureViews) {
        this.featureViews = featureViews;
    }

    public Long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Long totalCount) {
        this.totalCount = totalCount;
    }
}
