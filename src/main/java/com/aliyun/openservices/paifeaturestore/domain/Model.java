package com.aliyun.openservices.paifeaturestore.domain;

import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.aliyun.openservices.paifeaturestore.model.ModelFeatures;
import com.aliyun.tea.utils.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Model {
    private final com.aliyun.openservices.paifeaturestore.model.Model model;

    private final Project project;

    private final Map<String, FeatureView> featureViewMap = new HashMap<>();

    private final Map<String, FeatureEntity> featureEntityMap = new HashMap<>();

    // featureview : feature names
    private final Map<String, List<String>> featureNamesMap = new HashMap<>();

    // featureview : alias names
    private final Map<String, Map<String, String>> aliasNamesMap = new HashMap<>();

    // feature entity joinid : featureviews
    private final Map<String, Map<String, FeatureView>> featureEntityJoinIdMap = new HashMap<>();

    List<String> featureEntityJoinIdList = new ArrayList<>();

    static ExecutorService executorService;

    static {
        executorService = Executors.newCachedThreadPool(r -> {
            Thread thread = new Thread(r);
            thread.setName("modelfeature-processor");
            thread.setDaemon(true);
            return thread;
        });
    }

    public Model(com.aliyun.openservices.paifeaturestore.model.Model model, Project project) {
        this.model = model;
        this.project = project;

        for (ModelFeatures feature : this.model.getFeatures()) {
            FeatureView featureView = project.getFeatureView(feature.getFeatureViewName());
            FeatureEntity featureEntity = project.getFeatureEntity(featureView.getFeatureView().getFeatureEntityName());

            this.featureViewMap.put(feature.getFeatureViewName(), featureView);
            this.featureEntityMap.put(featureView.getFeatureView().getFeatureEntityName(), featureEntity);

            if (this.featureNamesMap.containsKey(feature.getFeatureViewName())) {
                this.featureNamesMap.get(feature.getFeatureViewName()).add(feature.getName());
            } else {
                List<String> names = new ArrayList<>();
                names.add(feature.getName());
                this.featureNamesMap.put(feature.getFeatureViewName(), names);
            }

            if (!StringUtils.isEmpty(feature.getAliasName())) {
                if (this.aliasNamesMap.containsKey(feature.getFeatureViewName())) {
                    this.aliasNamesMap.get(feature.getFeatureViewName()).put(feature.getName(), feature.getAliasName());
                } else {
                    Map<String, String> names = new HashMap<>();
                    names.put(feature.getName(), feature.getAliasName());
                    this.aliasNamesMap.put(feature.getFeatureViewName(), names);
                }
            }

            if (this.featureEntityJoinIdMap.containsKey(featureEntity.getFeatureEntity().getFeatureEntityJoinid())) {
                this.featureEntityJoinIdMap.get(featureEntity.getFeatureEntity().getFeatureEntityJoinid()).put(feature.getFeatureViewName(), featureView);
            } else {
                Map<String, FeatureView> featureViewMap1 = new HashMap<>();
                featureViewMap1.put(feature.getFeatureViewName(), featureView);
                this.featureEntityJoinIdMap.put(featureEntity.getFeatureEntity().getFeatureEntityJoinid(), featureViewMap1);
            }

            for (String joinId : this.featureEntityJoinIdMap.keySet()) {
                this.featureEntityJoinIdList.add(joinId);
            }
        }
    }

    public com.aliyun.openservices.paifeaturestore.model.Model getModel() {
        return model;
    }

    public FeatureResult getOnlineFeatures(Map<String, List<String>> joinIds) throws Exception {
        int size = -1;
        for (String joinId : this.featureEntityJoinIdList) {
            if (!joinIds.containsKey(joinId)) {
                throw new RuntimeException(String.format("join id:%s not found", joinId));
            }
            if (-1 == size) {
                size = joinIds.get(joinId).size();
            } else {
                if (size != joinIds.get(joinId).size()) {
                    throw new RuntimeException(String.format("join id:%s length not equal", joinId));
                }
            }

        }

        CountDownLatch countDownLatch = new CountDownLatch(joinIds.size());
        // thread safe map
        Map<String, List<FeatureResult>> joinIdFeaturesMap = new ConcurrentHashMap<>();
        for (Map.Entry<String, List<String>> entry : joinIds.entrySet()) {
            executorService.execute(() -> {
                try {
                    Map<String, FeatureView> featureViewMap = this.featureEntityJoinIdMap.get(entry.getKey());
                    Stream<CompletableFuture<FeatureResult>> completableFutureStream = featureViewMap.values().stream().map(featureView -> CompletableFuture.supplyAsync(() -> {
                        try {
                            return featureView.getOnlineFeatures(entry.getValue().toArray(new String[0]),
                                    this.featureNamesMap.get(featureView.getFeatureView().getName()).toArray(new String[0]), this.aliasNamesMap.get(featureView.getFeatureView().getName()));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }));

                    List<FeatureResult> featureResults = completableFutureStream.map(CompletableFuture::join).collect(Collectors.toList());
                    // add to map
                    joinIdFeaturesMap.put(entry.getKey(), featureResults);
                } finally {
                    countDownLatch.countDown();
                }
            });
        }

        // wait all featureview get features
        countDownLatch.await();
        FeatureStoreResult featureStoreResult = new FeatureStoreResult();
        List<String> featureFields = new ArrayList<>();
        Map<String, FSType> featureFieldTypeMap = new HashMap<>();
        List<Map<String, Object>> featureDataList = new ArrayList<>();

        for (String joinId : joinIds.keySet()) {
            for (FeatureResult result : joinIdFeaturesMap.get(joinId)) {
                featureFields.addAll(Arrays.asList(result.getFeatureFields()));
                featureFieldTypeMap.putAll(result.getFeatureFieldTypeMap());
            }
        }
        for (int i = 0; i < size; i++) {
            Map<String, Object> featuresMap = new HashMap<>();
            for (String joinId : this.featureEntityJoinIdList) {
                String joinIdValue = joinIds.get(joinId).get(i);
                for (FeatureResult result : joinIdFeaturesMap.get(joinId)) {
                    for (Map<String, Object> featureData : result.getFeatureData()) {
                        if (joinIdValue.equals(String.valueOf(featureData.get(joinId)))) {
                            featuresMap.putAll(featureData);
                        }
                    }
                }
            }
            featureDataList.add(featuresMap);
        }


        featureStoreResult.setFeatureFields(featureFields.toArray(new String[0]));
        featureStoreResult.setFeatureDataList(featureDataList);
        featureStoreResult.setFeatureFieldTypeMap(featureFieldTypeMap);
        return featureStoreResult;
    }

    /**
     * Get features with featureEntityName. JoinIds must contain the joinId of the  FeatureEntity.
     * @param joinIds
     * @param featureEntityName
     * @return
     * @throws Exception
     */
    public FeatureResult getOnlineFeaturesWithEntity(Map<String, List<String>> joinIds, String featureEntityName) throws Exception {
        FeatureEntity featureEntity = this.featureEntityMap.get(featureEntityName);
        if (featureEntity == null) {
            throw new RuntimeException(String.format("feature entity name:%s not found", featureEntityName));
        }

        if (!joinIds.containsKey(featureEntity.getFeatureEntity().getFeatureEntityJoinid())) {
            throw new RuntimeException(String.format("join id:%s not found", featureEntity.getFeatureEntity().getFeatureEntityJoinid()));
        }

        Map<String, FeatureView> featureViewMap = this.featureEntityJoinIdMap.get(featureEntity.getFeatureEntity().getFeatureEntityJoinid());

        Stream<CompletableFuture<FeatureResult>> completableFutureStream = featureViewMap.values().stream().map(featureView -> CompletableFuture.supplyAsync(() -> {
            try {
                return featureView.getOnlineFeatures(joinIds.get(featureEntity.getFeatureEntity().getFeatureEntityJoinid()).toArray(new String[0]),
                        this.featureNamesMap.get(featureView.getFeatureView().getName()).toArray(new String[0]), this.aliasNamesMap.get(featureView.getFeatureView().getName()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));

        List<FeatureResult> featureResults = completableFutureStream.map(CompletableFuture::join).collect(Collectors.toList());

        FeatureStoreResult featureStoreResult = new FeatureStoreResult();
        List<String> featureFields = new ArrayList<>();
        Map<String, FSType> featureFieldTypeMap = new HashMap<>();
        List<Map<String, Object>> featureDataList = new ArrayList<>();

        for (FeatureResult result : featureResults) {
            featureFields.addAll(Arrays.asList(result.getFeatureFields()));
            featureFieldTypeMap.putAll(result.getFeatureFieldTypeMap());
        }

        for (String joinIdValue : joinIds.get(featureEntity.getFeatureEntity().getFeatureEntityJoinid())) {
            Map<String, Object> featuresMap = new HashMap<>();
            for (FeatureResult result : featureResults) {
                for (Map<String, Object> featureData : result.getFeatureData()) {
                    if (joinIdValue.equals(String.valueOf(featureData.get(featureEntity.getFeatureEntity().getFeatureEntityJoinid())))) {
                        featuresMap.putAll(featureData);
                    }
                }
            }
            featureDataList.add(featuresMap);
        }

        featureStoreResult.setFeatureFields(featureFields.toArray(new String[0]));
        featureStoreResult.setFeatureDataList(featureDataList);
        featureStoreResult.setFeatureFieldTypeMap(featureFieldTypeMap);
        return featureStoreResult;
    }
}
