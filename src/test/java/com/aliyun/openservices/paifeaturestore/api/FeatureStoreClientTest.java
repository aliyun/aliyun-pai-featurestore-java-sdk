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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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

        FeatureResult features =  seqFeatureView.getOnlineFeatures( new String[]{"10", "100020289","100027794"});
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

        joinids.put("user_id", Arrays.asList(new String[]{"100023406", "100020289"}));
        features = model.getOnlineFeaturesWithEntity(joinids, "user");

        while (features.next()) {
            for (String name : features.getFeatureFields()) {
                System.out.print(String.format("%s=%s,", name, features.getObject(name)));
            }
            System.out.println();
        }
    }
    @Ignore
    @Test
    public void otsDataTest2() throws Exception {
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
        int count = 222;
        String[] joinIds = new String[count];
        for (int i=0; i < count-1; i++) {
            joinIds[i] = "100000132";
        }
        joinIds[count-1] = "100001167";

        FeatureResult features = featureView.getOnlineFeatures(joinIds, new String[]{"gender", "age", "city"}, null);

        if (features.getFeatureData().size() != count) {
            throw new Exception("request size not equal");
        }
        while (features.next()) {
            for (String name : features.getFeatureFields()) {
                System.out.print(String.format("%s=%s,", name, features.getObject(name)));
            }
            System.out.println("");
        }

        for (int i = 0; i < 10; i++) {
            Instant start = Instant.now();
            features = featureView.getOnlineFeatures(joinIds, new String[]{"gender", "age", "city"}, null);

            while (features.next()) {
                for (String name : features.getFeatureFields()) {
                    //System.out.print(String.format("%s=%s,", name, features.getObject(name)));
                }
            }
            Instant end = Instant.now();
            Duration timeElapsed = Duration.between(start, end);
            System.out.println("次数:" + (i+1) + " 执行耗时（毫秒）：" + timeElapsed.toMillis());
        }

    }
    @Ignore
    @Test
    public void featureDBTest() throws Exception {
        Configuration configuration = new Configuration("cn-beijing",
                Constants.accessId, Constants.accessKey,"tablestore_p2" );

        configuration.setUsername(Constants.username);
        configuration.setPassword(Constants.password);

        configuration.setDomain(Constants.host);

        ApiClient client = new ApiClient(configuration);

        FeatureStoreClient featureStoreClient = new FeatureStoreClient(client, Constants.usePublicAddress);

        Project project = featureStoreClient.getProject("tablestore_p2");
        if (null == project) {
            throw  new RuntimeException("project not found");
        }

        FeatureView featureView = project.getFeatureView("user_fea3");
        if (null == featureView) {
            throw  new RuntimeException("featureview not found");
        }
        int count = 3;
        String[] joinIds = new String[count];
        joinIds[0] = "7718078399602073545";
        joinIds[1] = "782486411886831247";
        joinIds[2] = "2855275313274611949";

        for (int i = 0; i < 1000;i++) {
            long startTime = System.nanoTime();
            FeatureResult features = featureView.getOnlineFeatures(joinIds );

            if (features.getFeatureData().size() != count) {
                throw new Exception("request size not equal");
            }
            while (features.next()) {
                for (String name : features.getFeatureFields()) {
                    System.out.print(String.format("%s=%s,", name, features.getObject(name)));
                }
                System.out.println("");
            }

            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            double durationInMilliseconds = duration / 1_000_000.0;

            System.out.println("操作耗时：" + durationInMilliseconds + "ms");
        }


    }
    @Ignore
    @Test
    public void writeTofeatureDBTest() throws Exception {
        Configuration configuration = new Configuration("cn-beijing",
                Constants.accessId, Constants.accessKey,"fs_demo_featuredb" );

        configuration.setUsername(Constants.username);
        configuration.setPassword(Constants.password);

        configuration.setDomain(Constants.host);

        ApiClient client = new ApiClient(configuration);

        FeatureStoreClient featureStoreClient = new FeatureStoreClient(client, Constants.usePublicAddress);

        Project project = featureStoreClient.getProject("fs_demo_featuredb");
        if (null == project) {
            throw  new RuntimeException("project not found");
        }

        FeatureView featureView = project.getFeatureView("user_test_1");
        if (null == featureView) {
            throw  new RuntimeException("featureview not found");
        }

        int int_field = 3456;
        List<Map<String, Object>> data = new ArrayList<>();
        Map<String, Object> m1 = new HashMap<>();
        m1.put("user_id", 123);
        m1.put("string_field", "male");
        m1.put("int32_field", int_field);
        m1.put("float_field", 0.25f);
        m1.put("double_field", 0.75d);
        m1.put("boolean_field", true);
        data.add(m1);
        featureView.writeFeatures(data);

        Thread.sleep(1000);
        FeatureResult features = featureView.getOnlineFeatures( new String[]{"123"} );

        while (features.next()) {
            for (String name : features.getFeatureFields()) {
                if (name.equals("user_id")) {
                    if (features.getLong(name) != 123) {
                        throw new Exception("get feature error");
                    }
                } else if (name.equals("string_field")) {
                    if (!features.getString(name).equals("male")) {
                        throw new Exception("get feature error");
                    }
                } else if (name.equals("int32_field")) {
                    if (features.getInt(name) != int_field) {
                        throw new Exception("get feature error");
                    }

                }
                System.out.print(String.format("%s=%s,", name, features.getObject(name)));
            }
            System.out.println("");
        }



    }
}
