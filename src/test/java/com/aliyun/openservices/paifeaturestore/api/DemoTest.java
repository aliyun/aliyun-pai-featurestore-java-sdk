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

public class DemoTest {
    //测试Hologres数据
    @Ignore
    @Test
    public void hologrestest(){

        try {
            //创建配置类
            Configuration cf = new Configuration("cn-hangzhou",Constants.accessId,Constants.accessKey,"ele28");
            cf.setDomain(Constants.host);//默认vpc环境，现在是本机
            //准备客户端
            ApiClient apiClient = new ApiClient(cf);
            //FS客户端
            FeatureStoreClient featureStoreClient = new FeatureStoreClient(apiClient);
            //获取project
            Project project=featureStoreClient.getProject("ele28");
            if(project==null){
                throw new RuntimeException("Project not found");
            }
            //获取project的特征视图featureView
            FeatureView featureView=project.getFeatureView("mc_test");
            if (featureView == null) {
                throw  new RuntimeException("FeatureView not found");
            }

            Map<String,String> m1=new HashMap<>();
            m1.put("gender","gender1");
            //获取线上特征
            FeatureResult featureResult1=featureView.getOnlineFeatures(new String[]{"100017768","100027781","100072534"},new String[]{"*"},m1);

            //输出特征信息
            while(featureResult1.next()){
                for(String m:featureResult1.getFeatureFields()){//特征名
                    System.out.print(String.format("%s='%s'(%s) ",m,featureResult1.getObject(m),featureResult1.getType(m)));
                }
                System.out.println("---------------");
            }

            //获取模型
            Model model=project.getModelFeature("model_t1");
            if(model==null){
                throw new RuntimeException("Model not found");
            }

            Map<String, List<String>> m2=new HashMap<>();
            m2.put("user_id",Arrays.asList("101683057","100664753"));
            m2.put("item_id",Arrays.asList("203665415","201805122"));

            System.out.println(" 有FeatureEntity ");
            //输出特征信息（加特征实体）
            FeatureResult featureResult2 = model.getOnlineFeaturesWithEntity(m2,"user");

            while(featureResult2.next()){
                for(String name:featureResult2.getFeatureFields()){//特征名
                    System.out.print(String.format("%s='%s' ",name,featureResult2.getObject(name)));
                }
                System.out.println("---------------");
            }

            //输出特征信息（不加特征实体）
            System.out.println(" 无FeatureEntity ");
            FeatureResult featureResult3 = model.getOnlineFeatures(m2);

            while(featureResult3.next()) {
                for (String name : featureResult3.getFeatureFields()) {//特征名
                    System.out.print(String.format("%s='%s' ", name, featureResult3.getObject(name)));
                }
                System.out.println("---------------");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

 			@Test
    public void hologresdatatest2() throws Exception {
        //准备配置类
        Configuration configuration = new Configuration("cn-hangzhou",Constants.accessId,Constants.accessKey,"dec6");
        configuration.setDomain(Constants.host);//默认是vpc环境，现在是本机，所以需设置
        //注册api客户端
        ApiClient apiClient = new ApiClient(configuration);
        //注册FS客户端
        FeatureStoreClient featureStoreClient = new FeatureStoreClient(apiClient);
        //获取项目
        Project dec6 = featureStoreClient.getProject("dec6");
        if(dec6==null){//判空
            throw new RuntimeException("Not found this project");

        }
        //获取特征视图
        FeatureView zh1 = dec6.getFeatureView("zh1");
        if(zh1==null){
            throw new RuntimeException("Not found this featureview");
        }
        HashMap<String, String> ss = new HashMap<>();
        ss.put("gender","gender1");
        //获取指定user_id的指定显示列的数据
        FeatureResult features = zh1.getOnlineFeatures(new String[]{"101597737", "101683773", "101724226"}, new String[]{"*"}, ss);
        //输出db
        System.out.println("---------------------------------------------------------------------------");
        while (features.next()){
            for (String name:features.getFeatureFields()){
                System.out.printf(String.format("%s=%s(%s) ",name,features.getObject(name),features.getType(name)));
            }
            System.out.println();
        }
        System.out.println("---------------------------------------------------------------------------");
        //item侧
        FeatureView zh2 = dec6.getFeatureView("zh2");
        if(zh1==null){
            throw new RuntimeException("Not found this featureview");
        }
        HashMap<String, String> ssh = new HashMap<>();

//        ssh.put("item_id","itemId");
        FeatureResult features1 = zh2.getOnlineFeatures(new String[]{"200111753", "200124053", "200318864"}, new String[]{"*"},ssh);
        while (features1.next()){
            for(String name:features1.getFeatureFields()){
                System.out.printf(String.format("%s=%s(%s) ",name,features1.getObject(name),features1.getType(name)));
            }
            System.out.println();
        }

        //获取模型特征
        Model model_t1 = dec6.getModelFeature("model_t1");
        if(model_t1==null){//判空
            throw new RuntimeException("Not found model");
        }
        HashMap<String, List<String>> map = new HashMap<>();
        map.put("user_id",Arrays.asList("101597737", "101683773","101724226"));
        map.put("item_id",Arrays.asList("200111753","200124053","200318864"));
        //获取item侧的特征实体
        FeatureResult item = model_t1.getOnlineFeaturesWithEntity(map, "user");
        System.out.println("---------------------------------------------------------------------------");
        while (item.next()){
            for (String name:item.getFeatureFields()){
                System.out.printf(String.format("%s=%s ",name,item.getObject(name)));
            }
            System.out.println();
        }
        System.out.println("---------------------------------------------------------------------------");
        //获取所有的
        FeatureResult all = model_t1.getOnlineFeatures(map);
        System.out.println("---------------------------------------------------------------------------");
        while (all.next()){
            for (String name:all.getFeatureFields()){
                System.out.printf(String.format("%s=%s ",name,all.getObject(name)));
            }
            System.out.println();
        }
        System.out.println("---------------------------------------------------------------------------");
    }

    //TableStore
    @Test
    public void otsDatasourceTest() throws Exception {
        //注册配置类
        Configuration configuration = new Configuration("cn-hangzhou",Constants.accessId,
                Constants.accessKey,"dec8");
        configuration.setDomain(Constants.host);
        ApiClient apiClient = new ApiClient(configuration);
        FeatureStoreClient fsclient = new FeatureStoreClient(apiClient);

        //获取项目
        Project dec8 = fsclient.getProject("dec8");
        if(dec8==null){
            throw new RuntimeException("This project is not exist");
        }

        //获取特征视图
        //客户侧
        FeatureView mo1 = dec8.getFeatureViewMap().get("mo1");
        if(mo1==null){
            throw new RuntimeException("This featureView is not exist");
        }
        //获取部分线上数据
        HashMap<String, String> ss = new HashMap<>();
        FeatureResult features = mo1.getOnlineFeatures(new String[]{"101598051", "101598471", "101601287"}, new String[]{"*"}, ss);
        //输出
        while (features.next()){
            for(String name:features.getFeatureFields()){
                System.out.printf("%s=%s(%s) ",name,features.getObject(name),features.getType(name));
            }
            System.out.println();
        }

        //服务侧
        FeatureView mo2 = dec8.getFeatureViewMap().get("mo2");
        if(mo2==null){
            throw new RuntimeException("This featureView is not exist");
        }
        //获取线上数据
        FeatureResult features1 = mo2.getOnlineFeatures(new String[]{"200004157", "200006185", "200034730"});
        //输出
        while (features1.next()){
            for(String name:features1.getFeatureFields()){
                System.out.printf("%s=%s(%s) ",name,features1.getObject(name),features1.getType(name));
            }
            System.out.println();
        }

        //获取模型特征
        Model mf1 = dec8.getModelFeature("mf1");
        HashMap<String, List<String>> mm = new HashMap<>();
        mm.put("user_id", Arrays.asList("101598051", "101598471", "101601287"));
        mm.put("item_id",Arrays.asList("200004157", "200006185", "200034730"));
        //获取所有关联的实体包含的数据
        FeatureResult fr1 = mf1.getOnlineFeatures(mm);
        while(fr1.next()){
            for(String f:fr1.getFeatureFields()){
                System.out.printf("%s=%s ",f,fr1.getObject(f));
            }
            System.out.println();
        }
        //仅获取item侧实体包含的数据
        FeatureResult fr2 = mf1.getOnlineFeaturesWithEntity(mm, "server");
        while(fr2.next()){
            for(String f:fr2.getFeatureFields()){
                System.out.printf("%s=%s ",f,fr2.getObject(f));
            }
            System.out.println();
        }
    }
}
