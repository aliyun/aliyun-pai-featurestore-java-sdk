# aliyun-pai-featurestore-java-sdk


[PAI-FeatureStore 文档](https://help.aliyun.com/zh/pai/user-guide/featurestore-overview?spm=a2c4g.11186623.0.0.60b5329099wa5f)

## 导入依赖
```
<dependency>
  <groupId>com.aliyun.openservices.aiservice</groupId>
  <artifactId>paifeaturestore-sdk</artifactId>
  <version>1.0.3</version>
</dependency>
```
## 使用方式

### 1. 初始化客户端

```java
public class Constants {
    public static String accessId = "";
    public static String accessKey = "";

    static {
        accessId = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
        accessKey = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
    }
}

// 配置regionId、accessId、accessKey以及项目名称
Configuration configuration = new Configuration("cn-hangzhou",Constants.accessId,Constants.accessKey,"dec6");  
ApiClient apiClient = new ApiClient(configuration);  
FeatureStoreClient featureStoreClient = new FeatureStoreClient(apiClient);  
```

由于 SDK 是直连 onlinestore 的， client 需要在 VPC 环境运行。 比如 hologres/graphcompute , 需要在指定的 VPC 才能连接。

初始化FeatureStoreClient客户端时，默认入参false代表 VPC 环境运行。
如果是本地调试（非vpc环境），入参改为true即可。
```java
// 如果是本地调试，vpc 连接不通的话，可以直接使用公网地址, 但生产环境，一定要用 vpc 地址 
FeatureStoreClient featureStoreClient = new FeatureStoreClient(apiClient, true);  

// FeatureStoreClient 也需要与 FeatureStore Server 进行交互，获取各种元数据信息。默认也是通过 vpc 进行连接，如果通过公网，可以显式的设置
configuration.setDomain("FeatureStore Server 的公网地址");
```

| 地域              | vpc 地址                                     | 公网地址                                 |
| ----------------- | -------------------------------------------- | ---------------------------------------- |
| 北京(cn-beijing)  | paifeaturestore-vpc.cn-beijing.aliyuncs.com  | paifeaturestore.cn-beijing.aliyuncs.com  |
| 杭州(cn-hangzhou) | paifeaturestore-vpc.cn-hangzhou.aliyuncs.com | paifeaturestore.cn-hangzhou.aliyuncs.com |
| 上海(cn-shanghai) | paifeaturestore-vpc.cn-shanghai.aliyuncs.com | paifeaturestore.cn-shanghai.aliyuncs.com |
| 深圳(cn-shenzhen) | paifeaturestore-vpc.cn-shenzhen.aliyuncs.com | paifeaturestore.cn-shenzhen.aliyuncs.com |



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

//get sequencefeature by name
FeatureView hol_seq = dec15.getFeatureView("seq_test2");
      if(hol_seq==null){
          throw new RuntimeException("This featureView is not exist");
      } 
```
序列化特征数据
                      
```java
//序列化特征
SequenceFeatureView hol_seq = dec15.getSeqFeatureView("seq_test2");
if (hol_seq==null) {
    throw new RuntimeException("This featureView is not exist");
}
//获取序列化数据
FeatureResult features2 = hol_seq.getOnlineFeatures(new String[]{"100433245", "100433233"});
System.out.println("[");
for (Map<String, String> m:features2.getSeqfeatureDataList()) {
    System.out.println("{");
    for (Object key:m.keySet()){
        System.out.println(key+":"+m.get(key));
        System.out.print(",");
    }
    System.out.println("}");
}
System.out.println("]");

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

## 版本说明
### 1.0.4 (2024-03-13)
* 解决 tablestore 依赖 protofbuf 版本问题(参考[这里](https://help.aliyun.com/zh/tablestore/support/what-do-i-do-if-pb-library-conflicts-occur-when-i-use-tablestore-sdk-for-java))
 
### 1.0.3 (2024-02-20)
* 支持公网访问获取数据
* 支持序列特征获取

