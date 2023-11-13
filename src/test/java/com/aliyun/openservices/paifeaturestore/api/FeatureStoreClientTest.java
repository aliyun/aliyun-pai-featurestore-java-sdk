/*
 * Feature Store Restful Api
 * PAI-Feature Store is an ML-specific data system server
 *
 */

package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.openservices.paifeaturestore.FeatureStoreClient;
import com.aliyun.openservices.paifeaturestore.domain.FeatureResult;
import com.aliyun.openservices.paifeaturestore.domain.FeatureView;
import com.aliyun.openservices.paifeaturestore.domain.Model;
import com.aliyun.openservices.paifeaturestore.domain.Project;
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

        FeatureStoreClient featureStoreClient = new FeatureStoreClient(client);

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
        FeatureResult features = featureView.getOnlineFeatures(new String[]{"100020655", "100022124"} , new String[]{"age", "gender"}, alias);

        while (features.next()) {
            for (String name : features.getFeatureFields()) {
                System.out.print(String.format("%s=%s|%s,", name, features.getObject(name), features.getType(name)));
            }
            System.out.println("=======");
        }

        Model model = project.getModelFeature("rank");
        if (null == model) {
            throw  new RuntimeException("model not found");
        }

        Map<String, List<String>> joinids = new HashMap<>();

        joinids.put("user_id", Arrays.asList(new String[]{"100020655", "100022124"}));
        joinids.put("item_id", Arrays.asList(new String[]{"200034730", "200043342"}));
        features = model.getOnlineFeaturesWithEntity(joinids, "user");

        while (features.next()) {
            for (String name : features.getFeatureFields()) {
                System.out.print(String.format("%s=%s,", name, features.getObject(name)));
            }
            System.out.println("=======");
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

        FeatureStoreClient featureStoreClient = new FeatureStoreClient(client);

        Project project = featureStoreClient.getProject("fs_test_dj");
        if (null == project) {
            throw  new RuntimeException("project not found");
        }

        FeatureView featureView = project.getFeatureViewMap().get("user_fea");
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

        FeatureStoreClient featureStoreClient = new FeatureStoreClient(client);

        Project project = featureStoreClient.getProject("fs_igraph_test_dj");
        if (null == project) {
            throw  new RuntimeException("project not found");
        }

        FeatureView featureView = project.getFeatureViewMap().get("user_fea");
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
}
