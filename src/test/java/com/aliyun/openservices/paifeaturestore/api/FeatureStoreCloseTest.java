package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.openservices.paifeaturestore.FeatureStoreClient;
import com.aliyun.openservices.paifeaturestore.domain.FeatureResult;
import com.aliyun.openservices.paifeaturestore.domain.FeatureView;
import com.aliyun.openservices.paifeaturestore.domain.Project;

public class FeatureStoreCloseTest {
    public static void main(String[] args) {

        int count = 10;
        Thread[] threads = new Thread[count];
        for (int i = 0; i < count; i++) {
            threads[i] = new Thread(() -> {
                try {
                    for (int j = 0; j < 200; j++) {
                        Configuration configuration = new Configuration("cn-shenzhen",
                                Constants.accessId, Constants.accessKey,"fdb_test" );

                        configuration.setUsername(Constants.username);
                        configuration.setPassword(Constants.password);

                        configuration.setDomain("paifeaturestore.cn-shenzhen.aliyuncs.com");

                        ApiClient client = new ApiClient(configuration);

                        FeatureStoreClient featureStoreClient = new FeatureStoreClient(client, Constants.usePublicAddress );

                        Project project = featureStoreClient.getProject("fdb_test");
                        if (null == project) {
                            throw  new RuntimeException("project not found");
                        }

                        FeatureView featureView = project.getFeatureView("user_test_2");
                        if (null == featureView) {
                            throw  new RuntimeException("featureview not found");
                        }
                        String[] joinIds = new String[count];
                        for (int k=0; k < count; k++) {
                            joinIds[k] = String.valueOf(k);
                        }

                        FeatureResult features = featureView.getOnlineFeatures(joinIds  );

                        while (features.next()) {
                            for (String name : features.getFeatureFields()) {
                                //System.out.print(String.format("%s=%s,", name, features.getObject(name)));
                            }
                            //System.out.println("");
                        }

                        featureStoreClient.close();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        for (int i = 0; i < count; i++) {
            threads[i].start();
        }

        for (int i = 0; i < count; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
