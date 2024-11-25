package com.aliyun.openservices.paifeaturestore.dao;

import com.aliyun.openservices.paifeaturestore.domain.FeatureResult;
import com.aliyun.openservices.paifeaturestore.model.FeatureViewSeqConfig;

import java.util.List;
import java.util.Map;

/*  This class defines the specification (method) for obtaining the feature view.*/
public interface FeatureViewDao {
    //  Select the display fields according to keys to get the feature result set.
    FeatureResult getFeatures(String[] keys, String[] selectFields );

    //  Gets a result set of serialized feature fields based on keys
    FeatureResult getSequenceFeatures(String[] keys, String userIdField, FeatureViewSeqConfig featureViewSeqConfig);

    default void writeFeatures(List<Map<String, Object>> data) {
        throw new RuntimeException("FeatureViewDao not support");
    };

   default void writeFlush() {
        throw new RuntimeException("FeatureViewDao not support");
    };

   default void close() throws Exception {
   }

}
