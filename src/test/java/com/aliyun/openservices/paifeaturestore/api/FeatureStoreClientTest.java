/*
 * Feature Store Restful Api
 * PAI-Feature Store is an ML-specific data system server
 *
 */

package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.openservices.paifeaturestore.FeatureStoreClient;
import com.aliyun.openservices.paifeaturestore.domain.*;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class FeatureStoreClientTest {

    @Ignore
    @Test
    public void hologresDataTest() throws Exception {
        Configuration configuration = new Configuration("cn-beijing",
                Constants.accessId, Constants.accessKey,"fs_holo_dj" );

        configuration.setDomain(Constants.host);

        ApiClient client = new ApiClient(configuration);

        FeatureStoreClient featureStoreClient = new FeatureStoreClient(client, Constants.usePublicAddress);

        Project project = featureStoreClient.getProject("fs_holo_dj");
        if (null == project) {
            throw  new RuntimeException("project not found");
        }

        FeatureView featureView = project.getFeatureView("user_fea");
        if (null == featureView) {
            throw  new RuntimeException("featureview not found");
        }

        Map<String, String> alias = new HashMap<>();
        alias.put("gender", "gender1");
        FeatureResult features = featureView.getOnlineFeatures(new String[]{"100051703", "100069505"} , new String[]{"age", "gender"}, alias);

        while (features.next()) {
            for (String name : features.getFeatureFields()) {
                System.out.print(String.format("%s=%s|%s,", name, features.getObject(name), features.getType(name)));
            }
            System.out.println();
        }

        System.out.println("get features from model");
        Model model = project.getModelFeature("rank");
        if (null == model) {
            throw  new RuntimeException("model not found");
        }

        Map<String, List<String>> joinids = new HashMap<>();

        joinids.put("user_id", Arrays.asList(new String[]{"100051703", "100069505"}));
        joinids.put("item_id", Arrays.asList(new String[]{"200034730", "200043342"}));
        features = model.getOnlineFeaturesWithEntity(joinids, "user");

        while (features.next()) {
            for (String name : features.getFeatureFields()) {
                System.out.print(String.format("%s=%s,", name, features.getObject(name)));
            }
            System.out.println();
        }

        features = model.getOnlineFeatures(joinids);

        while (features.next()) {
            for (String name : features.getFeatureFields()) {
                System.out.print(String.format("%s=%s,", name, features.getObject(name)));
            }
            System.out.println("=======");
        }
    }

    @Ignore
    @Test
    public void otsDataTest() throws Exception {
        Configuration configuration = new Configuration("cn-beijing",
                Constants.accessId, Constants.accessKey,"fs_test_dj" );

        configuration.setDomain(Constants.host);

        ApiClient client = new ApiClient(configuration);

        FeatureStoreClient featureStoreClient = new FeatureStoreClient(client, Constants.usePublicAddress);

        Project project = featureStoreClient.getProject("fs_test_dj");
        if (null == project) {
            throw  new RuntimeException("project not found");
        }

        FeatureView featureView = project.getFeatureView("user_fea");
        if (null == featureView) {
            throw  new RuntimeException("featureview not found");
        }

        FeatureResult features = featureView.getOnlineFeatures(new String[]{"100000132", "100001167"} );

        while (features.next()) {
            for (String name : features.getFeatureFields()) {
                System.out.print(String.format("%s=%s,", name, features.getObject(name)));
            }
            System.out.println("");
        }


    }

    @Ignore
    @Test
    public void igraphDataTest() throws Exception {
        Configuration configuration = new Configuration("cn-beijing",
                Constants.accessId, Constants.accessKey,"fs_igraph_test_dj" );

        configuration.setDomain(Constants.host);

        ApiClient client = new ApiClient(configuration);

        FeatureStoreClient featureStoreClient = new FeatureStoreClient(client, Constants.usePublicAddress);

        Project project = featureStoreClient.getProject("fs_igraph_test_dj");
        if (null == project) {
            throw  new RuntimeException("project not found");
        }

        FeatureView featureView = project.getFeatureView("user_fea");
        if (null == featureView) {
            throw  new RuntimeException("featureview not found");
        }

        FeatureResult features = featureView.getOnlineFeatures(new String[]{"100000132", "100001167"}, new String[]{"age", "tags", "user__avg_playtime_7d" }, null );

        while (features.next()) {
            for (String name : features.getFeatureFields()) {
                System.out.print(String.format("%s=%s,", name, features.getObject(name)));
            }
            System.out.println("");
        }


    }
    @Ignore
    @Test
    public void sequenceFeatureViewTest() throws Exception {
        Configuration cf = new Configuration("cn-beijing", Constants.accessId, Constants.accessKey, "fs_demo2");
        cf.setDomain(Constants.host);//默认vpc环境，现在是本机

        ApiClient client = new ApiClient(cf);

        FeatureStoreClient featureStoreClient = new FeatureStoreClient(client,Constants.usePublicAddress);

        Project project = featureStoreClient.getProject("fs_demo2");
        if (null == project) {
            throw new RuntimeException("project not found");
        }

        SequenceFeatureView seqFeatureView =  project.getSeqFeatureView("wide_seq_feature_v3");
        if (null == seqFeatureView) {
            throw new RuntimeException("sequence feature view not found");
        }

        FeatureResult features =  seqFeatureView.getOnlineFeatures( new String[]{"10", "100023406","100027794"});
        while (features.next()) {
            for (String name : features.getFeatureFields()) {
                System.out.print(String.format("%s=%s,", name, features.getObject(name)));
            }
            System.out.println("");
        }

        System.out.println("get features from model");
        Model model = project.getModelFeature("fs_rank_v2");
        if (null == model) {
            throw  new RuntimeException("model not found");
        }

        Map<String, List<String>> joinids = new HashMap<>();

        joinids.put("user_id", Arrays.asList(new String[]{"100023406", "119387221"}));
        features = model.getOnlineFeaturesWithEntity(joinids, "user");

        while (features.next()) {
            for (String name : features.getFeatureFields()) {
                System.out.print(String.format("%s=%s,", name, features.getObject(name)));
            }
            System.out.println();
        }
    }
}
