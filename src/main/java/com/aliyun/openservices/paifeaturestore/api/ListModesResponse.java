package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.openservices.paifeaturestore.model.Model;

import java.util.List;

public class ListModesResponse {
    Long totalCount ;
    List<Model> models;

    public Long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Long totalCount) {
        this.totalCount = totalCount;
    }

    public List<Model> getModels() {
        return models;
    }

    public void setModels(List<Model> models) {
        this.models = models;
    }
}
