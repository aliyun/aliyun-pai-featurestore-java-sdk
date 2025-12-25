package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.openservices.paifeaturestore.FeatureStoreClient;
import com.aliyun.openservices.paifeaturestore.domain.*;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Time;
import java.util.*;

public class DemoTest {

    @Ignore
    @org.junit.Test
    public void hologrestest3(){
        try {
            //创建配置类
            Configuration cf = new Configuration("cn-hangzhou", Constants.accessId, Constants.accessKey, "JAN11");
            cf.setDomain(Constants.host);//默认vpc环境，现在是本机
            //准备客户端
            ApiClient apiClient = new ApiClient(cf);
            //FS客户端
            FeatureStoreClient featureStoreClient = new FeatureStoreClient(apiClient,true);
            //获取project
            Project project = featureStoreClient.getProject("JAN11");
            if (project == null) {
                throw new RuntimeException("Project not found");
            }
            System.out.println(project.getProject().getProjectName());

            FeatureView fv1 = project.getFeatureView("fv1");
            if (fv1==null) {
                throw new RuntimeException("This featureView is not exist");
            }
            System.out.println(fv1.getFeatureView().getName());

            FeatureResult features = fv1.getOnlineFeatures(new String[]{"100495729", "100320022"});
            while (features.next()) {
                for (String name : features.getFeatureFields()) {
                    System.out.print(String.format("%s=%s,", name, features.getObject(name)));
                }
                System.out.println("");
            }

            SequenceFeatureView seqfv4 =  project.getSeqFeatureView("seq_fv4");
            if (null == seqfv4) {
                throw  new RuntimeException("Sequence feature view not found");
            }

            FeatureResult features2 =  seqfv4.getOnlineFeatures( new String[]{"100217707", "100138881"});
            System.out.println("[");
            for (Map<String,Object> m:features2.getFeatureData()) {
                System.out.println("{");
                for (Object key:m.keySet()) {
                    System.out.println(key+":"+m.get(key));
                }
                System.out.println("}");
            }
            System.out.println("]");

            HashMap<String, List<String>> fsmap = new HashMap<>();
            fsmap.put("user_id",Arrays.asList("100039333","100161956"));
            fsmap.put("item_id",Arrays.asList(new String[]{"200011231","200156550"}));

            Model mdt2 = project.getModelFeature("seq_mdt1");
            if (mdt2==null) {
                throw new RuntimeException("ModelFeature is not exist!");
            }

            FeatureResult fr4 = mdt2.getOnlineFeatures(fsmap);
            System.out.println("[");
            for (Map<String, Object> m:fr4.getFeatureData()) {
                System.out.println("{");
                for (Object key:m.keySet()) {
                    System.out.println(key+":"+m.get(key));
                }
                    System.out.println("}");
            }
            System.out.printf("]");


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //TableStore
    @org.junit.Test
    @Ignore
    public void otsDatasourceTest() throws Exception {
        //注册配置类
        Configuration configuration = new Configuration("cn-hangzhou",Constants.accessId,
                Constants.accessKey,"dec8");
        configuration.setDomain(Constants.host);
        ApiClient apiClient = new ApiClient(configuration);
        FeatureStoreClient fsclient = new FeatureStoreClient(apiClient,Constants.usePublicAddress);//true:本地测试

        //获取项目
        Project dec8 = fsclient.getProject("dec8");
        if (dec8==null) {
            throw new RuntimeException("This project is not exist");
        }

        SequenceFeatureView otsSeq3 =  dec8.getSeqFeatureView("ots_seq3");
        if (null == otsSeq3) {
            throw  new RuntimeException("sequence feature view not found");
        }

        FeatureResult features2 =  otsSeq3.getOnlineFeatures(new String[]{"100000142", "100003474"});
        System.out.println("[");
        for (Map<String,Object> m:features2.getFeatureData()) {
            System.out.println("{");
            for (Object key:m.keySet()) {
                System.out.println(key+":"+m.get(key));
            }
            System.out.println("}");
        }
        System.out.println("]");


        //获取模型特征(包含序列化特征)
        Model mdt1 = dec8.getModelFeature("mdt1");
        HashMap<String, List<String>> mm1 = new HashMap<>();
        mm1.put("user_id", Arrays.asList("100001167", "100003495", "100003520"));

        FeatureResult fs = mdt1.getOnlineFeatures(mm1);
        System.out.println("[");
        for (Map<String, Object> m:fs.getFeatureData()) {
            System.out.println("{");
            for (Object key:m.keySet()) {
                System.out.println(key+":"+m.get(key));
                System.out.print(",");
            }
            System.out.println("}");
        }
        System.out.printf("]");

    }

    @Test
    @Ignore
    public void igraphDatasourceTest() throws Exception {
        //注册配置类
        Configuration configuration = new Configuration("cn-hangzhou",Constants.accessId,
                Constants.accessKey,"dec8two");
        configuration.setDomain(Constants.host);
        ApiClient apiClient = new ApiClient(configuration);
        FeatureStoreClient fsclient = new FeatureStoreClient(apiClient,true);

        //获取项目
        Project dec8 = fsclient.getProject("dec8two");
        if (dec8==null) {
            throw new RuntimeException("This project is not exist");
        }
        //获取特征视图
        //客户侧
        FeatureView mg1 = dec8.getFeatureView("mg1");
        if (mg1==null) {
            throw new RuntimeException("This featureView is not exist");
        }
        //获取部分线上数据
        HashMap<String, String> ss = new HashMap<>();
        FeatureResult features = mg1.getOnlineFeatures(new String[]{"101598051", "101598471", "101601287"}, new String[]{"*"}, ss);
        //输出
        while (features.next()) {
            System.out.println("-------------------------");
            for (String name:features.getFeatureFields()) {
                System.out.printf("%s=%s(%s) ",name,features.getObject(name),features.getType(name));
            }
            System.out.println();
        }

        //服务侧
        FeatureView mo2 = dec8.getFeatureView("mg2");
        if (mo2==null) {
            throw new RuntimeException("This featureView is not exist");
        }

        //获取线上数据
        FeatureResult features1 = mo2.getOnlineFeatures(new String[]{"200004157", "200006185", "200034730"});
        //输出
        while (features1.next()) {
            System.out.println("-------------------------");
            for (String name:features1.getFeatureFields()) {
                System.out.printf("%s=%s(%s) ",name,features1.getObject(name),features1.getType(name));
            }
            System.out.println();
        }
        System.out.println("-------------------------");

        //实时特征视图
        FeatureView fv1 = dec8.getFeatureView("fv1");
        if (fv1==null) {
            throw new RuntimeException("This featureView is not exist");
        }
        //获取部分线上数据
        FeatureResult rf = fv1.getOnlineFeatures(new String[]{"35d3d5a52a7515c2ca6bb4d8e965149b", "0ab7e3efacd56983f16503572d2b9915","84dfd3f91dd85ea105bc74a4f0d7a067"}, new String[]{"*"}, ss);
        //输出
        while (rf.next()) {
            for (String name:rf.getFeatureFields()) {
                System.out.printf("%s=%s(%s) ",name,rf.getObject(name),rf.getType(name));
            }
            System.out.println();
        }

        //序列特征侧
        SequenceFeatureView seq2 = dec8.getSeqFeatureView("seq2");
        if (seq2==null) {
            throw new RuntimeException("This featureView is not exist");
        }
        FeatureResult features2 = seq2.getOnlineFeatures(new String[]{"183806135", "108725121"});
        System.out.println("[");
        for (Map<String, Object> m:features2.getFeatureData()) {
            System.out.println("{");
            for (Object key:m.keySet()) {
                System.out.print(key+":"+m.get(key));
                System.out.println(",");
            }
            System.out.println("}");
        }
        System.out.println("]");

//        获取模型特征
        Model mf1 = dec8.getModelFeature("model_test1");
        HashMap<String, List<String>> mm = new HashMap<>();
        mm.put("user_id", Arrays.asList("101598051", "101598471", "101601287"));
        mm.put("item_id",Arrays.asList("200004157", "200006185", "200034730"));

//        获取所有关联的实体包含的数据
        FeatureResult fr1 = mf1.getOnlineFeatures(mm);
        while (fr1.next()) {
            System.out.println("-------------------------");
            for(String f:fr1.getFeatureFields()){
                System.out.printf("%s=%s ",f,fr1.getObject(f));
            }
            System.out.println();
        }
        System.out.println("-------------------------");

//        仅获取item侧实体包含的数据
        FeatureResult fr2 = mf1.getOnlineFeaturesWithEntity(mm, "server");
        while (fr2.next()) {
            System.out.println("-------------------------");
            for (String f:fr2.getFeatureFields()) {
                System.out.printf("%s=%s ",f,fr2.getObject(f));
            }
            System.out.println();
        }
        System.out.println("-------------------------");

//        获取模型特征(包含序列化特征部分)
        Model mf2 = dec8.getModelFeature("model_test2");
        if (mf2==null) {
            throw new RuntimeException("This modelFeature is not exist");
        }
        HashMap<String, List<String>> fsmap = new HashMap<>();
        fsmap.put("user_id",Arrays.asList("100001167","100024146"));
        fsmap.put("item_id",Arrays.asList(new String[]{"200138790","200385417"}));

        FeatureResult fr3 = mf2.getOnlineFeatures(fsmap);
        System.out.println("[");
        for (Map<String, Object> m:fr3.getFeatureData()) {
            System.out.println("{");
            for (Object key:m.keySet()) {
                System.out.println(key+":"+m.get(key));
                System.out.print(",");
            }
            System.out.println("}");
        }
        System.out.printf("]");

    }

    @Test
    public void childFeatureEntityTest() throws Exception {
        String projectName = "fdb_test_case";
        String featureViewName = "item_base_table2";
        String childModelFeatureName = "model_with_child_entity";
        String childModelFeatureName2 = "model_with_child_entity2";
        Configuration config = new Configuration("cn-beijing",
                Constants.accessId, Constants.accessKey, projectName);
        config.setDomain(Constants.host);
        config.setUsername(Constants.username);
        config.setPassword(Constants.password);

        ApiClient apiClient = new ApiClient(config);
        FeatureStoreClient client = new FeatureStoreClient(apiClient,true);
        if (client == null) {
            throw new RuntimeException("This project is not exist");
        }
        Project project = client.getProject(projectName);
        if (project == null) {
            throw new RuntimeException("This project is not exist");
        }
        FeatureEntity featureEntity = project.getFeatureEntity("author");
        if (featureEntity == null) {
            throw new RuntimeException("This featureEntity is not exist");
        }

        //有上级
        if (featureEntity.getFeatureEntity().getParentFeatureEntityId() != null && featureEntity.getFeatureEntity().getParentFeatureEntityId() != 0) {
            FeatureEntity parentChildEntity = project.getFeatureEntity(featureEntity.getFeatureEntity().getParentFeatureEntityName());
            System.out.println(parentChildEntity);
        }


        FeatureView itemFeatureView = project.getFeatureView(featureViewName);
        if (itemFeatureView == null){
            throw new RuntimeException("This featureView is not exist");
        }

       //写入特征
//        List<Map<String, Object>> writeData = new ArrayList<>();
//        for(int i=0;i<10;i++){
//            HashMap<String, Object> mapData = new HashMap<>();
//            int random = new Random().nextInt(1000000);
//            mapData.put("item_id",i+1);
//            mapData.put("author_id",random);
//            mapData.put("another_id",random*(i+1));
//            mapData.put("event_time",System.currentTimeMillis());
//            writeData.add(mapData);
//        }
//
//        for (Iterator<Map<String, Object>> it = writeData.iterator(); it.hasNext();) {
//            System.out.println(it.next());
//        }
//
//        for (int i=0;i<10;i++){
//            itemFeatureView.writeFeatures(writeData);
//        }
//        Thread.sleep(5000);


        FeatureResult onlineFeaturesFv = itemFeatureView.getOnlineFeatures(new String[]{"1", "2", "3"}, new String[]{"*"}, null);
        while (onlineFeaturesFv.next()) {
            System.out.println("-------------------------");
            for (String f:onlineFeaturesFv.getFeatureFields()) {
                System.out.printf("%s=%s ",f,onlineFeaturesFv.getObject(f));
            }
            System.out.println();
        }


//        FeatureView userFeatureView = project.getFeatureView("user_fv1216");
//        if (userFeatureView == null){
//            throw new RuntimeException("This featureView is not exist");
//        }
//
//        FeatureResult onlineFeaturesFv2 = userFeatureView.getOnlineFeatures(new String[]{"122195575", "189269449", "105601516"}, new String[]{"*"}, null);
//        while (onlineFeaturesFv2.next()) {
//            System.out.println("-------------------------");
//            for (String f:onlineFeaturesFv2.getFeatureFields()) {
//                System.out.printf("%s=%s ",f,onlineFeaturesFv2.getObject(f));
//            }
//            System.out.println();
//        }



        //2.创建的model_feature的包含下级entity的特征
        Model modelFeature = project.getModelFeature(childModelFeatureName);
        if (modelFeature==null){
            throw new RuntimeException("This modelFeature is not exist");
        }
        FeatureResult onlineFeatures1 = modelFeature.getOnlineFeaturesWithEntity(new HashMap<String, List<String>>(){{
            put("item_id",Arrays.asList("1", "2", "3", "4"));}},"item");
        if (onlineFeatures1.getFeatureData()!=null){
            for (Map<String, Object> m:onlineFeatures1.getFeatureData()) {
                System.out.println("-------------------------");
                for (Object key:m.keySet()) {
                    System.out.printf("%s=%s ",key,m.get(key));
                }
                System.out.println();
            }
        }

        FeatureResult onlineFeatures2 = modelFeature.getOnlineFeaturesWithEntity(new HashMap<String, List<String>>(){{
            put("item_id",Arrays.asList("4", "5", "6","7"));}},"item");
        if (onlineFeatures2.getFeatureData()!=null){
            for (Map<String, Object> m:onlineFeatures2.getFeatureData()) {
                System.out.println("-------------------------");
                for (Object key:m.keySet()) {
                    System.out.printf("%s=%s ",key,m.get(key));
                }
                System.out.println();
            }
        }

//        //（2）get下级entity的特征
        FeatureResult onlineFeatures3 = modelFeature.getOnlineFeaturesWithEntity(new HashMap<String, List<String>>(){{
            put("author_id",Arrays.asList("1001", "1002", "1003"));}},"author");
        if (onlineFeatures3.getFeatureData()!=null){
            for (Map<String, Object> m:onlineFeatures3.getFeatureData()) {
                System.out.println("-------------------------");
                for (Object key:m.keySet()) {
                    System.out.printf("%s=%s ",key,m.get(key));
                }
                System.out.println();
            }
        }

        Model modelFeature2 = project.getModelFeature(childModelFeatureName2);
        if (modelFeature2==null){
            throw new RuntimeException("This modelFeature is not exist");
        }

        FeatureResult onlineFeatures4 = modelFeature2.getOnlineFeaturesWithEntity(new HashMap<String, List<String>>(){{
            put("item_id",Arrays.asList("1", "2", "3", "4", "6", "8", "9"));}},"item");
        if (onlineFeatures4.getFeatureData()!=null){
            for (Map<String, Object> m:onlineFeatures4.getFeatureData()) {
                System.out.println("-------------------------");
                for (Object key:m.keySet()) {
                    System.out.printf("%s=%s ",key,m.get(key));
                }
                System.out.println();
            }
        }



    }
}
