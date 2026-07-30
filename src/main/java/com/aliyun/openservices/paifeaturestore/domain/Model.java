package com.aliyun.openservices.paifeaturestore.domain;

import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.aliyun.openservices.paifeaturestore.model.ModelFeatures;
import com.aliyun.openservices.paifeaturestore.model.SeqConfig;
import com.aliyun.tea.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Model {
    public static Logger logger = LoggerFactory.getLogger(Model.class);
    private final com.aliyun.openservices.paifeaturestore.model.Model model;

    private final Project project;

    private final Map<String, IFeatureView> featureViewMap = new HashMap<>();


    private final Map<String, FeatureEntity> featureEntityMap = new HashMap<>();

    private final Map<String, Set<String>> parentHaveChildFeatureEntityMap = new HashMap<>();

    // featureview : feature names
    private final Map<String, List<String>> featureNamesMap = new HashMap<>();

    // featureview : alias names
    private final Map<String, Map<String, String>> aliasNamesMap = new HashMap<>();

    // feature entity joinid : featureviews
    private final Map<String, Map<String, IFeatureView>> featureEntityJoinIdMap = new HashMap<>();

    List<String> featureEntityJoinIdList = new ArrayList<>();

    // entity joinid : feature entity
    private final Map<String, FeatureEntity> entityJoinIdToFeatureEntityMap = new HashMap<>();

    private ExecutorService executorService;

    public Model(com.aliyun.openservices.paifeaturestore.model.Model model, Project project) {
        int parallelism = Runtime.getRuntime().availableProcessors();
        executorService = new ThreadPoolExecutor( parallelism*2,Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,new SynchronousQueue<>(), r -> {
            Thread thread = new Thread(r);
            thread.setName("modelfeature-processor");
            thread.setDaemon(true);
            return thread;

        });

        this.model = model;
        this.project = project;
        
        for (ModelFeatures feature : this.model.getFeatures()) {
            IFeatureView featureView = project.getFeatureView(feature.getFeatureViewName());
            if (null == featureView) {
                featureView = project.getSeqFeatureView(feature.getFeatureViewName());
            }
            FeatureEntity featureEntity = project.getFeatureEntity(featureView.getFeatureView().getFeatureEntityName());

            this.featureViewMap.put(feature.getFeatureViewName(), featureView);
            this.featureEntityMap.put(featureView.getFeatureView().getFeatureEntityName(), featureEntity);
            if (featureEntity.getFeatureEntity().getParentFeatureEntityId() != null && featureEntity.getFeatureEntity().getParentFeatureEntityId() != 0 ){//有上级，一对多
                String parentFeatureEntityName = featureEntity.getFeatureEntity().getParentFeatureEntityName();
                String childFeatureEntityJoinid = featureEntity.getFeatureEntity().getFeatureEntityJoinid();
                if (this.parentHaveChildFeatureEntityMap.containsKey(parentFeatureEntityName)){
                    this.parentHaveChildFeatureEntityMap.get(parentFeatureEntityName).add(childFeatureEntityJoinid);
                }else {
                    HashSet<String> joinIds = new HashSet<>();
                    joinIds.add(childFeatureEntityJoinid);
                    this.parentHaveChildFeatureEntityMap.put(parentFeatureEntityName,joinIds);
                }
            }

            this.entityJoinIdToFeatureEntityMap.put(featureEntity.getFeatureEntity().getFeatureEntityJoinid(), featureEntity);

            if (this.featureNamesMap.containsKey(feature.getFeatureViewName())) {
                if (featureView instanceof  SequenceFeatureView) {
                    SequenceFeatureView sequenceFeatureView = (SequenceFeatureView) featureView;
                    for (SeqConfig config : sequenceFeatureView.getSeqConfigs()) {
                        if (config.getOfflineSeqName().equals(feature.getName())) {
                            this.featureNamesMap.get(feature.getFeatureViewName()).add(config.getOnlineSeqName());
                        }
                    }
                } else {
                    this.featureNamesMap.get(feature.getFeatureViewName()).add(feature.getName());
                }
            } else {
                List<String> names = new ArrayList<>();
                if (featureView instanceof  SequenceFeatureView) {
                    SequenceFeatureView sequenceFeatureView = (SequenceFeatureView) featureView;
                    for (SeqConfig config : sequenceFeatureView.getSeqConfigs()) {
                        if (config.getOfflineSeqName().equals(feature.getName())) {
                            names.add(config.getOnlineSeqName());
                        }

                    }
                } else {
                    names.add(feature.getName());
                }
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
                Map<String, IFeatureView> featureViewMap1 = new HashMap<>();
                featureViewMap1.put(feature.getFeatureViewName(), featureView);
                this.featureEntityJoinIdMap.put(featureEntity.getFeatureEntity().getFeatureEntityJoinid(), featureViewMap1);
            }

            Integer parentId = featureEntity.getFeatureEntity().getParentFeatureEntityId();
            if (parentId == null || parentId == 0) {
                String joinId = featureEntity.getFeatureEntity().getFeatureEntityJoinid();
                if (!this.featureEntityJoinIdList.contains(joinId)) {
                    this.featureEntityJoinIdList.add(joinId);
                }
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

        // thread safe map
        List<String> featureFields = new CopyOnWriteArrayList<>();
        Map<String, FSType> featureFieldTypeMap = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        Map<Integer, Map<String, Object>> indexFeatrueMap = new ConcurrentHashMap<>();
        for (Map.Entry<String, List<String>> entry : joinIds.entrySet()) {
            int finalSize = size;
            CompletableFuture<Void> future = CompletableFuture.runAsync(()->{
                try {
                    FeatureResult featureResult = new FeatureStoreResult();
                    try {
                        featureResult = getOnlineFeaturesWithEntity(joinIds, this.entityJoinIdToFeatureEntityMap.get(entry.getKey()).featureEntity.getFeatureEntityName());
                    } catch (Exception e) {
                        logger.error("featureview get online features error", e);
                    }

                    featureFields.addAll(Arrays.asList(featureResult.getFeatureFields()));
                    if (featureResult.getFeatureFieldTypeMap()!=null) {
                        featureFieldTypeMap.putAll(featureResult.getFeatureFieldTypeMap());
                    }
                    for (int i = 0; i < finalSize; i++) {
                        String joinIdValue = entry.getValue().get(i);
                        for (Map<String, Object> featureData : featureResult.getFeatureData()) {
                            if (featureData != null) {
                                if ( joinIdValue.equals(String.valueOf(featureData.get(entry.getKey())))) {
                                    indexFeatrueMap.computeIfAbsent(i, k -> new ConcurrentHashMap<>()).putAll(featureData);
                                    break;
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("featureview get online features error", e);
                }
            }, executorService);
            futures.add(future);
        }

        FeatureStoreResult featureStoreResult = new FeatureStoreResult();
        List<Map<String, Object>> featureDataList = new ArrayList<>(size);
        // wait all featureview get features
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        for (int i = 0; i < size; i++) {
            if (indexFeatrueMap.get(i)==null) {
                featureDataList.add(new HashMap<>());
            } else {
                featureDataList.add(indexFeatrueMap.get(i));
            }
        }


        featureStoreResult.setFeatureFields(featureFields.toArray(new String[0]));
        featureStoreResult.setFeatureDataList(featureDataList);
        featureStoreResult.setFeatureFieldTypeMap(featureFieldTypeMap);
        return featureStoreResult;
    }

    public FeatureResult getOnlineFeaturesWithEntity(Map<String, List<String>> joinIds, String featureEntityName){
        FeatureEntity featureEntity = this.featureEntityMap.get(featureEntityName);
        if (featureEntity == null) {
            throw new RuntimeException(String.format("feature entity name:%s not found", featureEntityName));
        }
        String entityJoinId = featureEntity.getFeatureEntity().getFeatureEntityJoinid();
        if (!joinIds.containsKey(entityJoinId)) {
            throw new RuntimeException(String.format("join id:%s not found", entityJoinId));
        }

        Map<String, IFeatureView> featureViewMap = this.featureEntityJoinIdMap.get(entityJoinId);//得到的是item_id对应的featureView
        Set<String> childFeatureEntitiesJoinid;
        //有下级entity，需要一并获取下级entity的featureViewMap以及entityJoinId
        Integer parentId = featureEntity.getFeatureEntity().getParentFeatureEntityId();
        if(parentId == null || parentId == 0){//上级 item(author、another)
            if (this.parentHaveChildFeatureEntityMap.containsKey(featureEntity.getFeatureEntity().getFeatureEntityName())){
                //得到模型特征所有包含的子entity(author、another)
                childFeatureEntitiesJoinid = this.parentHaveChildFeatureEntityMap.get(featureEntity.getFeatureEntity().getFeatureEntityName());//author_id
            } else {
                childFeatureEntitiesJoinid = new HashSet<>();
            }
        } else {
            childFeatureEntitiesJoinid = new HashSet<>();
        }


        String[] joinIdsArray = joinIds.get(entityJoinId).toArray(new String[0]);
        FeatureStoreResult featureStoreResult = new FeatureStoreResult();
        List<String> featureFields = new CopyOnWriteArrayList<>();
        Map<String, FSType> featureFieldTypeMap = new ConcurrentHashMap<>();
        Map<String, Map<String, Object>> joinIdFeaturMap = new ConcurrentHashMap<>();


        List<CompletableFuture<Void>> futures = new ArrayList<>();
        // Collect child joinIds across all featureViews to deduplicate RPC calls
        Map<String, Set<String>> aggregatedChildJoinIds = new ConcurrentHashMap<>();
        for (IFeatureView featureView : featureViewMap.values()) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(()->{
                try {
                    FeatureResult featureResult =   featureView.getOnlineFeatures(joinIdsArray,
                            this.featureNamesMap.get(featureView.getFeatureView().getName()).toArray(new String[0]), this.aliasNamesMap.get(featureView.getFeatureView().getName()));

                    if (featureResult.getFeatureData()!=null) {
                        featureFields.addAll(Arrays.asList(featureResult.getFeatureFields()));
                        if (featureResult.getFeatureFieldTypeMap()!=null) {
                            featureFieldTypeMap.putAll(featureResult.getFeatureFieldTypeMap());
                        }

                        for (Map<String, Object> featureData : featureResult.getFeatureData()) {
                            if (featureData != null) {
                                String joinIdValue = String.valueOf(featureData.get(entityJoinId));
                                // Filter out null-value entries
                                Map<String, Object> filteredData = featureData.entrySet().stream()
                                        .filter(entry -> entry.getValue() != null)
                                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                                joinIdFeaturMap.computeIfAbsent(joinIdValue, k -> new ConcurrentHashMap<>()).putAll(filteredData);

                                // Collect child entity joinIds for batch retrieval after all featureViews complete
                                for (String childFeatureEntityJoinId : childFeatureEntitiesJoinid) {
                                    if (featureData.containsKey(childFeatureEntityJoinId) && featureData.get(childFeatureEntityJoinId) != null) {
                                        String childFeatureEntityJoinIdValue = String.valueOf(featureData.get(childFeatureEntityJoinId));
                                        aggregatedChildJoinIds.computeIfAbsent(childFeatureEntityJoinId, k -> ConcurrentHashMap.newKeySet()).add(childFeatureEntityJoinIdValue);
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("get feature view features error", e);
                }
            }, executorService );
            futures.add(future);
        }

        // Wait for all featureView RPCs to complete before fetching child entity features
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // Fetch child entity features in a single batch to avoid duplicate RPCs across featureViews
        if (!aggregatedChildJoinIds.isEmpty()) {
            Map<String, List<FeatureResult>> childFeatureResults = processChildEntityFeatures(aggregatedChildJoinIds);
            for (Map.Entry<String, List<FeatureResult>> entry : childFeatureResults.entrySet()) {
                for (FeatureResult childFeatureResult : entry.getValue()) {
                    mergeChildFeatureData(childFeatureResult, entry.getKey(), featureFieldTypeMap, joinIdFeaturMap);
                }
            }
        }

        List<Map<String, Object>> featureDataList = new ArrayList<>();

        for (String joinIdValue : joinIdsArray) {
            if (joinIdFeaturMap.containsKey(joinIdValue)) {
                featureDataList.add(joinIdFeaturMap.get(joinIdValue));
            } else {
                Map<String, Object> featureMap = new HashMap<>();
                featureMap.put(entityJoinId, joinIdValue);
                featureDataList.add(featureMap);
            }
        }

        featureStoreResult.setFeatureFields(featureFields.toArray(new String[0]));
        featureStoreResult.setFeatureDataList(featureDataList);
        featureStoreResult.setFeatureFieldTypeMap(featureFieldTypeMap);

        return featureStoreResult;
    }

    private Map<String,List<FeatureResult>> processChildEntityFeatures(Map<String, Set<String>> childJoinIds) {
        Map<String, List<FeatureResult>> featureResultsForChildFeatureEntityJoinIdMap = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : childJoinIds.entrySet()) {
            String childFeatureEntityJoinId = entry.getKey();
            Set<String> childJoinIdValues = entry.getValue();
            List<FeatureResult> featureResults = processChildFeatureViews(childFeatureEntityJoinId, childJoinIdValues);
            featureResultsForChildFeatureEntityJoinIdMap.put(childFeatureEntityJoinId, featureResults);
        }
        return featureResultsForChildFeatureEntityJoinIdMap;
    }

    private List<FeatureResult> processChildFeatureViews(String childFeatureEntityJoinId,
                                          Set<String> childJoinIdValues) {
        List<FeatureResult> featureResultsForChildFeatureEntityJoinId = new ArrayList<>();
        //多张featureView
        //多张featureView
        Map<String, IFeatureView> childFeatureViewMap = this.featureEntityJoinIdMap.get(childFeatureEntityJoinId);
        if (childFeatureViewMap != null) {
            String[] childJoinIdsArray = childJoinIdValues.toArray(new String[0]);
            for (IFeatureView childFeatureView : childFeatureViewMap.values()) {
                try {
                    FeatureResult childFeatureResult = childFeatureView.getOnlineFeatures(
                            childJoinIdsArray,
                            this.featureNamesMap.get(childFeatureView.getFeatureView().getName()).toArray(new String[0]),
                            this.aliasNamesMap.get(childFeatureView.getFeatureView().getName())
                    );
                    featureResultsForChildFeatureEntityJoinId.add(childFeatureResult);

                } catch (Exception e) {
                    logger.error("获取子实体特征失败: " + childFeatureEntityJoinId, e);
                }
            }
        }
        return featureResultsForChildFeatureEntityJoinId;
    }

    private void mergeChildFeatureData(FeatureResult childFeatureResult,
                                       String childFeatureEntityJoinId,
                                       Map<String, FSType> featureFieldTypeMap,
                                       Map<String, Map<String, Object>> joinIdFeatureMap) {
        if (childFeatureResult.getFeatureData() != null) {
            // 更新特征字段类型映射
            if (childFeatureResult.getFeatureFieldTypeMap() != null) {
                featureFieldTypeMap.putAll(childFeatureResult.getFeatureFieldTypeMap());
            }

            // 按 childJoinId 值建索引，避免 O(N×M) 全量扫描
            Map<String, List<Map<String, Object>>> childJoinIdIndex = new HashMap<>();
            for (Map<String, Object> parentData : joinIdFeatureMap.values()) {
                Object parentChildJoinId = parentData.get(childFeatureEntityJoinId);
                if (parentChildJoinId != null) {
                    childJoinIdIndex.computeIfAbsent(String.valueOf(parentChildJoinId), k -> new ArrayList<>()).add(parentData);
                }
            }

            for (Map<String, Object> featureData : childFeatureResult.getFeatureData()) {
                if (featureData != null && featureData.containsKey(childFeatureEntityJoinId)) {
                    Map<String, Object> filteredData = featureData.entrySet().stream()
                            .filter(entry -> entry.getValue() != null)
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                    Object childJoinIdValue = filteredData.get(childFeatureEntityJoinId);
                    if (childJoinIdValue != null) {
                        List<Map<String, Object>> matchingParents = childJoinIdIndex.get(String.valueOf(childJoinIdValue));
                        if (matchingParents != null) {
                            for (Map<String, Object> parentData : matchingParents) {
                                parentData.putAll(filteredData);
                            }
                        }
                    }
                }
            }
        }
    }



    public void close() throws Exception {
        if (!executorService.isShutdown()) {
            executorService.shutdown();
        }

        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }

        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }

}
