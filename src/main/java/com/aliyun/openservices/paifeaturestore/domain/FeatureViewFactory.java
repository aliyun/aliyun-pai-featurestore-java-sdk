package com.aliyun.openservices.paifeaturestore.domain;

public class FeatureViewFactory {

    public static IFeatureView getFeatureView(com.aliyun.openservices.paifeaturestore.model.FeatureView view, Project p, FeatureEntity featureEntity){
        if (view.getType().equals("Sequence")) {
            return new SequenceFeatureView(view,p,featureEntity);
        } else {
            return new FeatureView(view,p,featureEntity);
        }
    }
}
