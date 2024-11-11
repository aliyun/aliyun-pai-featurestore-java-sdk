/*
 * Feature Store Restful Api
 * PAI-Feature Store is an ML-specific data system server
 *
 */

package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.openservices.paifeaturestore.FeatureStoreClient;
import com.aliyun.openservices.paifeaturestore.constants.InsertMode;
import com.aliyun.openservices.paifeaturestore.domain.FeatureResult;
import com.aliyun.openservices.paifeaturestore.domain.FeatureView;
import com.aliyun.openservices.paifeaturestore.domain.Model;
import com.aliyun.openservices.paifeaturestore.domain.Project;
import com.aliyun.openservices.paifeaturestore.domain.SequenceFeatureView;
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
    public void featureDBDataSeqWriteTest() throws Exception {
        Configuration configuration = new Configuration("cn-beijing", Constants.accessId, Constants.accessKey, "ceci_fsdb_test");
        configuration.setDomain(Constants.host);
        configuration.setUsername(Constants.username);
        configuration.setPassword(Constants.password);
        ApiClient apiClient = new ApiClient(configuration);
        FeatureStoreClient featureStoreClient = new FeatureStoreClient(apiClient, true);
        Project project = featureStoreClient.getProject("ceci_fsdb_test");
        SequenceFeatureView sequenceFeatureView = project.getSeqFeatureView("sequence");
        List<Map<String, Object>> writeData = new ArrayList<>();
        Map<String, Object> data = new HashMap<>();
        data.put("request_id", 901850344);
        data.put("user_id", "172040759");
        data.put("page", "home");
        data.put("net_type", "wifi");
        data.put("day_h", 17);
        data.put("week_day", 6);
        data.put("event_unix_time", System.currentTimeMillis()/1000);
        data.put("item_id", "223466789");
        data.put("event","click");
        data.put("playtime", 54.7296554003366);
        writeData.add(data);
        sequenceFeatureView.writeFeatures(writeData);
        sequenceFeatureView.writeFlush();

    }
    @Ignore
    @Test
    public void featureDBDataSeqReadTest() throws Exception {
        Configuration configuration = new Configuration("cn-beijing", Constants.accessId, Constants.accessKey, "ceci_fsdb_test");
        configuration.setDomain(Constants.host);
        configuration.setUsername(Constants.username);
        configuration.setPassword(Constants.password);
        ApiClient apiClient = new ApiClient(configuration);
        FeatureStoreClient featureStoreClient = new FeatureStoreClient(apiClient, true);
        Project project = featureStoreClient.getProject("ceci_fsdb_test");
        SequenceFeatureView sequenceFeatureView = project.getSeqFeatureView("sequence");
        FeatureResult featureResult = sequenceFeatureView.getOnlineFeatures(new String[]{"172040759"}, new String[]{"*"}, null);
        while (featureResult.next()){
            for (String m:featureResult.getFeatureFields()){
                System.out.printf("%s='%s'(%s) ",m,featureResult.getObject(m),featureResult.getType(m));
            }
            System.out.println("---------------");

        }
    }
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

        FeatureResult features = featureView.getOnlineFeatures(new String[]{"100002419", "100001167", "12"} );

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
        Configuration configuration = new Configuration("cn-shenzhen",
                Constants.accessId, Constants.accessKey,"fdb_test" );

        configuration.setUsername(Constants.username);
        configuration.setPassword(Constants.password);

        configuration.setDomain("paifeaturestore.cn-shenzhen.aliyuncs.com");

        ApiClient client = new ApiClient(configuration);

        FeatureStoreClient featureStoreClient = new FeatureStoreClient(client, Constants.usePublicAddress);

        Project project = featureStoreClient.getProject("fdb_test");
        if (null == project) {
            throw  new RuntimeException("project not found");
        }

        FeatureView featureView = project.getFeatureView("user_test_1");
        if (null == featureView) {
            throw  new RuntimeException("featureview not found");
        }
        int count = 10;
        String[] joinIds = new String[count];
        for (int i=0; i < count; i++) {
            joinIds[i] = String.valueOf(i);
        }

        for (int i = 0; i < 100;i++) {
            long startTime = System.nanoTime();
            FeatureResult features = featureView.getOnlineFeatures(joinIds  );

            if (features.getFeatureData().size() != count) {
                //throw new Exception("request size not equal");
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
    public void featureDBWriteTest() throws Exception {
        Configuration configuration = new Configuration("cn-shenzhen",
                Constants.accessId, Constants.accessKey,"fdb_test" );

        configuration.setUsername(Constants.username);
        configuration.setPassword(Constants.password);

        configuration.setDomain("paifeaturestore.cn-shenzhen.aliyuncs.com");

        ApiClient client = new ApiClient(configuration);

        FeatureStoreClient featureStoreClient = new FeatureStoreClient(client, Constants.usePublicAddress);

        Project project = featureStoreClient.getProject("fdb_test");
        if (null == project) {
            throw  new RuntimeException("project not found");
        }

        FeatureView featureView = project.getFeatureView("user_test_2");
        if (null == featureView) {
            throw  new RuntimeException("featureview not found");
        }

        List<Map<String, Object>> writeData = new ArrayList<>();
        // add more data
        for (int i = 0; i < 10; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("user_id", i);
            data.put("string_field", String.format("test_%d", i));
            data.put("int32_field", i);
            data.put("int64_field", Long.valueOf(i));
            data.put("float_field", Float.valueOf(i));
            data.put("double_field", Double.valueOf(i));
            data.put("boolean_field", i % 2 == 0);
            writeData.add(data);
        }

        for (int i = 0; i < 100;i++) {
            long startTime = System.nanoTime();
            featureView.writeFeatures(writeData);

            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            double durationInMilliseconds = duration / 1_000_000.0;

            System.out.println("操作耗时：" + durationInMilliseconds + "ms");
            Thread.sleep(1000);
        }


        featureView.writeFlush();
    }
    @Ignore
    @Test
    public void featureDBComplexFeatureWriteTest() throws Exception {
        Configuration configuration = new Configuration("cn-shenzhen",
                Constants.accessId, Constants.accessKey,"fdb_test" );

        configuration.setUsername(Constants.username);
        configuration.setPassword(Constants.password);

        configuration.setDomain("paifeaturestore.cn-shenzhen.aliyuncs.com");

        ApiClient client = new ApiClient(configuration);

        FeatureStoreClient featureStoreClient = new FeatureStoreClient(client, Constants.usePublicAddress);

        Project project = featureStoreClient.getProject("fdb_test");
        if (null == project) {
            throw  new RuntimeException("project not found");
        }

        FeatureView featureView = project.getFeatureView("user_fea_complex2");
        if (null == featureView) {
            throw  new RuntimeException("featureview not found");
        }

        List<Map<String, Object>> writeData = new ArrayList<>();
        // add more data
        for (int i = 0; i < 10; i++) {
            Map<String, Object> data = new HashMap<>();
            Map<Integer, Double> mapIntDouble = new HashMap<>();
            mapIntDouble.put(i, i * 0.1d);
            mapIntDouble.put(i+1, i * 0.1d);
            Map<String, String> mapStringString = new HashMap<>();
            mapStringString.put(String.valueOf(i), String.valueOf(i));
            mapStringString.put(String.valueOf(i+1), String.valueOf(i+1));
            data.put("user_id", String.valueOf(i));
            data.put("array_int", Arrays.asList(i, i+1));
            data.put("array_float", Arrays.asList(i * 0.1f, i * 0.2f));
            data.put("map_int_double", mapIntDouble);
            data.put("map_string_string", mapStringString);
            writeData.add(data);
        }

        for (int i = 0; i < 100;i++) {
            long startTime = System.nanoTime();
            featureView.writeFeatures(writeData);

            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            double durationInMilliseconds = duration / 1_000_000.0;

            System.out.println("操作耗时：" + durationInMilliseconds + "ms");
        }


        Thread.sleep(1000);
        featureView.writeFlush();

    }
    @Ignore
    @Test
    public void featureDBComplexFeatrueReadTest() throws Exception {
        Configuration configuration = new Configuration("cn-shenzhen",
                Constants.accessId, Constants.accessKey,"fdb_test" );

        configuration.setUsername(Constants.username);
        configuration.setPassword(Constants.password);

        configuration.setDomain("paifeaturestore.cn-shenzhen.aliyuncs.com");

        ApiClient client = new ApiClient(configuration);

        FeatureStoreClient featureStoreClient = new FeatureStoreClient(client, Constants.usePublicAddress);

        Project project = featureStoreClient.getProject("fdb_test");
        if (null == project) {
            throw  new RuntimeException("project not found");
        }

        FeatureView featureView = project.getFeatureView("user_fea_complex2");
        if (null == featureView) {
            throw  new RuntimeException("featureview not found");
        }
        int count = 10;
        String[] joinIds = new String[count];
        for (int i=0; i < count; i++) {
            joinIds[i] = String.valueOf(i);
        }

        for (int i = 0; i < 100;i++) {
            long startTime = System.nanoTime();
            FeatureResult features = featureView.getOnlineFeatures(joinIds  );

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
    public void featureDBWriteWithModeTest() throws Exception {
        Configuration configuration = new Configuration("cn-shenzhen",
                Constants.accessId, Constants.accessKey,"fdb_test" );

        configuration.setUsername(Constants.username);
        configuration.setPassword(Constants.password);

        configuration.setDomain("paifeaturestore.cn-shenzhen.aliyuncs.com");

        ApiClient client = new ApiClient(configuration);

        FeatureStoreClient featureStoreClient = new FeatureStoreClient(client, Constants.usePublicAddress);

        Project project = featureStoreClient.getProject("fdb_test");
        if (null == project) {
            throw  new RuntimeException("project not found");
        }

        FeatureView featureView = project.getFeatureView("user_test_2");
        if (null == featureView) {
            throw  new RuntimeException("featureview not found");
        }

        List<Map<String, Object>> writeData = new ArrayList<>();
        // add more data
        for (int i = 0; i < 10; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("user_id", i);
            //data.put("string_field", "");
            data.put("int32_field", i + 100);
            //data.put("int64_field", Long.valueOf(i));
            //data.put("float_field", Float.valueOf(i));
            data.put("double_field", Double.valueOf(i) * 100);
            //data.put("boolean_field", i % 2 == 0);
            writeData.add(data);
        }

        for (int i = 0; i < 80;i++) {
            long startTime = System.nanoTime();
            featureView.writeFeatures(writeData, InsertMode.PartialFieldWrite);

            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            double durationInMilliseconds = duration / 1_000_000.0;

            System.out.println("操作耗时：" + durationInMilliseconds + "ms");
            Thread.sleep(1000);
        }

        FeatureView featureView4 = project.getFeatureView("user_test_4");
        if (null == featureView4) {
            throw  new RuntimeException("featureview4 not found");
        }

        for (int i = 0; i < 10;i++) {
            long startTime = System.nanoTime();
            featureView4.writeFeatures(writeData, InsertMode.PartialFieldWrite);

            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            double durationInMilliseconds = duration / 1_000_000.0;

            System.out.println("操作耗时：" + durationInMilliseconds + "ms");
            Thread.sleep(1000);
        }

        featureView.writeFlush();

    }
    @Ignore
    @Test
    public void modelGetFeaturesWithEntityTest() throws Exception {
        Configuration configuration = new Configuration("cn-shenzhen",
                Constants.accessId, Constants.accessKey,"fdb_test" );

        configuration.setDomain("paifeaturestore.cn-shenzhen.aliyuncs.com");
        configuration.setUsername(Constants.username);
        configuration.setPassword(Constants.password);
        ApiClient client = new ApiClient(configuration);

        FeatureStoreClient featureStoreClient = new FeatureStoreClient(client, Constants.usePublicAddress);

        Project project = featureStoreClient.getProject("fdb_test");
        if (null == project) {
            throw  new RuntimeException("project not found");
        }


        System.out.println("get features from model");
        Model model = project.getModelFeature("fs_test");
        if (null == model) {
            throw  new RuntimeException("model not found");
        }

        List<String> joinIds = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            joinIds.add(String.valueOf(i));
        }
        Map<String, List<String>> joinids = new HashMap<>();
        joinids.put("user_id", joinIds);
        long startTime = System.nanoTime();
        FeatureResult features = model.getOnlineFeaturesWithEntity(joinids, "user");

        while (features.next()) {
            for (String name : features.getFeatureFields()) {
                System.out.print(String.format("%s=%s,", name, features.getObject(name)));
            }
            System.out.println();
        }

        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        double durationInMilliseconds = duration / 1_000_000.0;
        System.out.println("操作耗时：" + durationInMilliseconds + "ms");
    }
    @Ignore
    @Test
    public void modelGetFeaturesTest() throws Exception {
        Configuration configuration = new Configuration("cn-shenzhen",
                Constants.accessId, Constants.accessKey,"fdb_test" );

        configuration.setDomain("paifeaturestore.cn-shenzhen.aliyuncs.com");
        configuration.setUsername(Constants.username);
        configuration.setPassword(Constants.password);
        ApiClient client = new ApiClient(configuration);

        FeatureStoreClient featureStoreClient = new FeatureStoreClient(client, Constants.usePublicAddress);

        Project project = featureStoreClient.getProject("fdb_test");
        if (null == project) {
            throw  new RuntimeException("project not found");
        }


        System.out.println("get features from model");
        Model model = project.getModelFeature("rank_test");
        if (null == model) {
            throw  new RuntimeException("model not found");
        }

        List<String> joinIds = new ArrayList<>();
        for (int i = 0; i < 101; i++) {
            joinIds.add(String.valueOf(i));
        }
        Map<String, List<String>> joinids = new HashMap<>();
        joinids.put("user_id", joinIds);
        joinids.put("item_id", joinIds);
        long startTime = System.nanoTime();
        FeatureResult features = model.getOnlineFeatures(joinids);

        while (features.next()) {
            for (String name : features.getFeatureFields()) {
                System.out.print(String.format("%s=%s,", name, features.getObject(name)));
            }
            System.out.println();
        }

        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        double durationInMilliseconds = duration / 1_000_000.0;
        System.out.println("操作耗时：" + durationInMilliseconds + "ms");
    }
}
