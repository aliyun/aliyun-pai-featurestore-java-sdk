# aliyun-pai-featurestore-java-sdk


[PAI-FeatureStore 文档](https://help.aliyun.com/zh/pai/user-guide/featurestore-overview?spm=a2c4g.11186623.0.0.60b5329099wa5f)

## 导入依赖
```
<dependency>
  <groupId>com.aliyun.openservices.aiservice</groupId>
  <artifactId>paifeaturestore-sdk</artifactId>
  <version>1.0.1</version>
</dependency>
```
## 使用方式

### 1. 初始化客户端

```java
public class Constants {
    public static String accessId = "";
    public static String accessKey = "";

    static {
        accessId = System.getenv("AccessId");
        accessKey = System.getenv("AccessKey");
    }
}

// 配置regionId、accessId、accessKey以及项目名称
Configuration cf = new Configuration("cn-hangzhou",Constants.accessId,Constants.accessKey,"dec6");  
ApiClient apiClient = new ApiClient(cf);  
FeatureStoreClient featureStoreClient = new FeatureStoreClient(apiClient);  
```

由于 SDK 是直连 onlinestore 的， client 需要在 VPC 环境运行。 比如 hologres/graphcompute , 需要在指定的 VPC 才能连接。

### 2. 获取 FeatureView 的特征数据
``` java
// get project by name
Project project=featureStoreClient.getProject("dec6");  
if(project==null){  
  throw new RuntimeException("Project not found");  
} 

// get feature view by name
FeatureView featureView=project.getFeatureView("zh1");  
if (featureView == null) {  
  throw new RuntimeException("FeatureView not found");  
}  

// get features 
FeatureResult featureResult1 = featureView.getOnlineFeatures(new String[]{"101597737", "101683773", "101724226"},new String[]{"*"},nil);
```
遍历返回的特征数据

```java
  //输出特征信息
  while(featureResult1.next()){
     for(String m:featureResult1.getFeatureFields()){// feature field name
         System.out.print(String.format("%s='%s'(%s) ",m,featureResult1.getObject(m),featureResult1.getType(m)));
     }
     System.out.println("---------------");
 }

```



### 3. 获取模型特征
``` java
// get model by name
Model model=project.getModelFeature("model_t1");  
if(model==null){  
		throw new RuntimeException("Model not found");   
}
// 以两个join_id实例(user_id,item_id)，传入的值个数需对应
Map<String, List<String>> m2=new HashMap<>();  
m2.put("user_id",Arrays.asList("101597737","101683773","101724226"));  
m2.put("item_id",Arrays.asList("200111753","200124053","200318864"));

// get features 
FeatureResult featureResult2 = model.getOnlineFeatures(m2);  
```
ModelFeature 可以关联多个 FeatureEntity, 可以设置多个 join_id, 然后特征统一返回。

示例中有两个 join_id, user_id 和 item_id 。 获取特征的时候需要设置相同的 id 数量。

也可以指定某个 FeatureEntity, 把 FeatureEntity 对应的特征一块返回。

```java
Map<String, List<String>> m3=new HashMap<>();  
m3.put("user_id",Arrays.asList("101597737","101683773","101724226"));  
FeatureResult featureResult3 = model.getOnlineFeaturesWithEntity(m3,"user");  
```

上面的含义是把 ModelFeature 下的 user(FeatureEntity) 对应的特征全部获取到。

