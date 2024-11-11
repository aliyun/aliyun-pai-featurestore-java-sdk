package com.aliyun.openservices.paifeaturestore.domain;

import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.aliyun.openservices.paifeaturestore.model.ModelFeatures;
import com.aliyun.openservices.paifeaturestore.model.SeqConfig;
import com.aliyun.tea.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    // featureview : feature names
    private final Map<String, List<String>> featureNamesMap = new HashMap<>();

    // featureview : alias names
    private final Map<String, Map<String, String>> aliasNamesMap = new HashMap<>();

    // feature entity joinid : featureviews
    private final Map<String, Map<String, IFeatureView>> featureEntityJoinIdMap = new HashMap<>();

    List<String> featureEntityJoinIdList = new ArrayList<>();

    // entity joinid : feature entity
    private final Map<String, FeatureEntity> entityJoinIdToFeatureEntityMap = new HashMap<>();

    static ExecutorService executorService;

    static {
        int parallelism = Runtime.getRuntime().availableProcessors();
        executorService = new ThreadPoolExecutor( parallelism*2,Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,new SynchronousQueue<>(), r -> {
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
            //IFeatureView featureView = project.getFeatureView(feature.getFeatureViewName());
            IFeatureView featureView = project.getFeatureView(feature.getFeatureViewName());
            if (null == featureView) {
                featureView = project.getSeqFeatureView(feature.getFeatureViewName());
            }
            FeatureEntity featureEntity = project.getFeatureEntity(featureView.getFeatureView().getFeatureEntityName());

            this.featureViewMap.put(feature.getFeatureViewName(), featureView);
            this.featureEntityMap.put(featureView.getFeatureView().getFeatureEntityName(), featureEntity);
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

    public FeatureResult getOnlineFeaturesWithEntity(Map<String, List<String>> joinIds, String featureEntityName) throws Exception {
        FeatureEntity featureEntity = this.featureEntityMap.get(featureEntityName);
        if (featureEntity == null) {
            throw new RuntimeException(String.format("feature entity name:%s not found", featureEntityName));
        }

        String entityJoinId = featureEntity.getFeatureEntity().getFeatureEntityJoinid();
        if (!joinIds.containsKey(entityJoinId)) {
            throw new RuntimeException(String.format("join id:%s not found", entityJoinId));
        }

        Map<String, IFeatureView> featureViewMap = this.featureEntityJoinIdMap.get(entityJoinId);

        String[] joinIdsArray = joinIds.get(entityJoinId).toArray(new String[0]);
        FeatureStoreResult featureStoreResult = new FeatureStoreResult();
        List<String> featureFields = new CopyOnWriteArrayList<>();
        Map<String, FSType> featureFieldTypeMap = new ConcurrentHashMap<>();
        Map<String, Map<String, Object>> joinIdFeaturMap = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
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
                                joinIdFeaturMap.computeIfAbsent(joinIdValue, k -> new ConcurrentHashMap<>()).putAll(featureData);
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("get feature view features error", e);
                }
            }, executorService );
            futures.add(future);
        }

        List<Map<String, Object>> featureDataList = new ArrayList<>();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

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


}
