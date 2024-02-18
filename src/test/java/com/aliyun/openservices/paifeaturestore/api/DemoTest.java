package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.openservices.paifeaturestore.FeatureStoreClient;
import com.aliyun.openservices.paifeaturestore.domain.*;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.jooq.meta.derby.sys.Sys;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

            IFeatureView fv1 = project.getFeatureView("fv1");
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

            IFeatureView seqfv1 =  project.getFeatureView("seq_fv3");
            if (null == seqfv1) {
                throw  new RuntimeException("Sequence feature view not found");
            }

            FeatureResult features2 =  seqfv1.getOnlineFeatures( new String[]{"10000001", "10000002"});
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
            fsmap.put("user_id",Arrays.asList("10000001","10000002"));
            Model mdt2 = project.getModelFeature("seq_mdt1");
            if (mdt2==null) {
                throw new RuntimeException("ModelFeature is not exist!");
            }
            fsmap.put("item_id",Arrays.asList(new String[]{"20000001","20000002"}));
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
    public void otsDatasourceTest() throws Exception {
        //注册配置类
        Configuration configuration = new Configuration("cn-hangzhou",Constants.accessId,
                Constants.accessKey,"dec8");
        configuration.setDomain(Constants.host);
        ApiClient apiClient = new ApiClient(configuration);
        FeatureStoreClient fsclient = new FeatureStoreClient(apiClient,true);//true:本地测试

        //获取项目
        Project dec8 = fsclient.getProject("dec8");
        if (dec8==null) {
            throw new RuntimeException("This project is not exist");
        }

        IFeatureView otsSeq3 =  dec8.getFeatureView("ots_seq3");
        if (null == otsSeq3) {
            throw  new RuntimeException("sequence feature view not found");
        }

        FeatureResult features2 =  otsSeq3.getOnlineFeatures(new String[]{"100001167", "100003520"});
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
        IFeatureView mo1 = dec8.getFeatureViewMap().get("mg1");
        if (mo1==null) {
            throw new RuntimeException("This featureView is not exist");
        }
        //获取部分线上数据
        HashMap<String, String> ss = new HashMap<>();
        FeatureResult features = mo1.getOnlineFeatures(new String[]{"101598051", "101598471", "101601287"}, new String[]{"*"}, ss);
        //输出
        while (features.next()) {
            System.out.println("-------------------------");
            for (String name:features.getFeatureFields()) {
                System.out.printf("%s=%s(%s) ",name,features.getObject(name),features.getType(name));
            }
            System.out.println();
        }

        //服务侧
        IFeatureView mo2 = dec8.getFeatureViewMap().get("mg2");
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
        IFeatureView fv1 = dec8.getFeatureViewMap().get("fv1");
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
}
