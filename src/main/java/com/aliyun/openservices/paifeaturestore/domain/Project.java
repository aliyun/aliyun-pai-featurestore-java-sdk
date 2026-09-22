package com.aliyun.openservices.paifeaturestore.domain;

import com.aliyun.openservices.paifeaturestore.api.ApiClient;
import com.aliyun.openservices.paifeaturestore.api.ListFeatureEntitiesResponse;
import com.aliyun.openservices.paifeaturestore.api.ListFeatureViewsResponse;
import com.aliyun.openservices.paifeaturestore.api.ListModesResponse;
import com.aliyun.openservices.paifeaturestore.constants.DatasourceType;
import com.aliyun.openservices.paifeaturestore.datasource.FeatureDBClient;
import com.aliyun.openservices.paifeaturestore.datasource.FeatureDBFactory;
import com.aliyun.openservices.paifeaturestore.datasource.Hologres;
import com.aliyun.openservices.paifeaturestore.datasource.HologresFactory;
import com.aliyun.openservices.paifeaturestore.datasource.IGraphFactory;
import com.aliyun.openservices.paifeaturestore.datasource.TableStoreFactory;
import com.aliyun.openservices.paifeaturestore.model.Datasource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Project {
    public static Logger logger = LoggerFactory.getLogger(Project.class);

    com.aliyun.openservices.paifeaturestore.model.Project project;

    private final OnlineStore onlineStore;

    private final Map<String, IFeatureView> featureViewMap = new ConcurrentHashMap<>();

    private final Map<String, FeatureEntity> featureEntityMap = new ConcurrentHashMap<>();

    private final Map<String, Model> modelMap = new ConcurrentHashMap<>();

    private boolean usePublicAddress = false;

    private String signature = null;

    private Datasource featureDBDatasource = null;

    private ApiClient apiClient;

    // 懒加载锁：避免 Flink 多线程并发触发同一元数据的重复 API 请求
    private final Object loadLock = new Object();

    // featureEntity 是否已完成过一次全量加载（project 可能没有 entity，不能用 map.isEmpty() 判断）
    private volatile boolean featureEntitiesLoaded = false;

    public Project(com.aliyun.openservices.paifeaturestore.model.Project project,boolean usePublicAddress) throws Exception {
        this.project = project;
        this.signature = project.getSignature();
        switch (project.getOnlineDatasourceType()) {
            case Datasource_Type_Hologres:
                HologresOnlinestore hologresOnlinestore = new HologresOnlinestore();
                hologresOnlinestore.setDatasource(project.getOnlineDataSource());
                this.onlineStore = hologresOnlinestore;
                if (null == HologresFactory.get(this.onlineStore.getDatasourceName())) {
                    Hologres hologres = new Hologres(hologresOnlinestore.getDatasource().generateDSN(DatasourceType.Datasource_Type_Hologres,usePublicAddress));
                    HologresFactory.register(this.onlineStore.getDatasourceName(), hologres);
                }
                break;
            case Datasource_Type_IGraph:
                IGraphOnlineStore iGraphOnlineStore = new IGraphOnlineStore();
                iGraphOnlineStore.setDatasource(project.getOnlineDataSource());
                this.onlineStore = iGraphOnlineStore;
                if (null == IGraphFactory.get(this.onlineStore.getDatasourceName())) {
                    IGraphFactory.register(this.onlineStore.getDatasourceName(),
                            iGraphOnlineStore.getDatasource().generateIgraphClient(usePublicAddress));
                }
                break;
            case Datasource_Type_TableStore:
                TableStoreOnlinestore tableStoreOnlinestore = new TableStoreOnlinestore();
                tableStoreOnlinestore.setDatasource(project.getOnlineDataSource());
                this.onlineStore = tableStoreOnlinestore;
                if (null == TableStoreFactory.get(this.onlineStore.getDatasourceName())) {
                    TableStoreFactory.register(this.onlineStore.getDatasourceName(),
                            tableStoreOnlinestore.getDatasource().generateOTSClient(usePublicAddress));
                }
                break;
            case Datasource_Type_FeatureDB:
                FeatureDBOnlinestore featureDBOnlinestore = new FeatureDBOnlinestore();
                featureDBOnlinestore.setDatasource(project.getOnlineDataSource());
                this.onlineStore = featureDBOnlinestore;
                if (null == FeatureDBFactory.get(this.onlineStore.getDatasourceName())) {
                    FeatureDBClient featureDBClient = featureDBOnlinestore.getDatasource().generateFeatureDBClient(usePublicAddress);
                    featureDBClient.setSignature(this.signature);
                    FeatureDBFactory.register(featureDBOnlinestore.getDatasourceName(), featureDBClient);
                }
                break;
            default:
                throw new RuntimeException("not support onlinestore type");
        }
    }


    public void setUsePublicAddress(boolean usePublicAddress) {
        this.usePublicAddress = usePublicAddress;
    }

    public FeatureView getFeatureView(String name) {
        if (!this.featureViewMap.containsKey(name)) {
            try {
                this.loadFeatureView(name);
            } catch (Exception e) {
                // fail-fast：加载失败(网络/权限/server 错误)时抛出并保留原始 cause，
                // 避免吞掉异常后下游拿到 null 产生远离根因的 NPE
                logger.error("load feature view {} error", name, e);
                throw new IllegalStateException(String.format("load feature view %s of project %s error", name, project.getProjectName()), e);
            }
        }
        IFeatureView featureView =  this.featureViewMap.get(name);
        if (featureView instanceof FeatureView) {
            return (FeatureView) featureView;
        }
        return null;
    }

    public SequenceFeatureView getSeqFeatureView(String name) {
        if (!this.featureViewMap.containsKey(name)) {
            try {
                this.loadFeatureView(name);
            } catch (Exception e) {
                logger.error("load feature view {} error", name, e);
                throw new IllegalStateException(String.format("load feature view %s of project %s error", name, project.getProjectName()), e);
            }
        }
        IFeatureView featureView = this.featureViewMap.get(name);
        if (featureView instanceof SequenceFeatureView) {
            return (SequenceFeatureView) featureView;
        }
        return null;
    }

    /**
     * 按名字从 FS server 拉取单个 featureView 并缓存到内存。
     * double-checked locking：并发调用同名时只有一个线程真正发起 API 请求。
     */
    private void loadFeatureView(String name) throws Exception {
        synchronized (loadLock) {
            if (this.featureViewMap.containsKey(name)) {
                return;
            }
            int pageNumber = 1;
            int pageSize = 100;
            do {
                ListFeatureViewsResponse listFeatureViewsResponse = this.apiClient.getFeatureViewApi().listFeatureViewsByName(name, String.valueOf(project.getProjectId()), pageNumber, pageSize);
                for (com.aliyun.openservices.paifeaturestore.model.FeatureView view : listFeatureViewsResponse.getFeatureViews()) {
                    com.aliyun.openservices.paifeaturestore.model.FeatureView featureView = this.apiClient.getFeatureViewApi().getFeatureViewById(String.valueOf(view.getFeatureViewId()));
                    if (featureView.getRegisterDatasourceId() > 0) {
                        Datasource registerDatasource = this.apiClient.getDatasourceApi().getDatasourceById(featureView.getRegisterDatasourceId());
                        featureView.setRegisterDatasource(registerDatasource);
                    }

                    IFeatureView domainFeatureView = FeatureViewFactory.getFeatureView(featureView, this, this.getFeatureEntity(featureView.getFeatureEntityName()));

                    this.addFeatureView(featureView.getName(), domainFeatureView);
                }

                if (listFeatureViewsResponse.getFeatureViews().size() == 0 || pageNumber * pageSize > listFeatureViewsResponse.getTotalCount()) {
                    break;
                }

                pageNumber++;
            } while (true);
        }
    }

    public FeatureEntity getFeatureEntity(String name) {
        if (!this.featureEntityMap.containsKey(name) && !this.featureEntitiesLoaded) {
            try {
                this.loadFeatureEntities();
            } catch (Exception e) {
                logger.error("load feature entity {} error", name, e);
                throw new IllegalStateException(String.format("load feature entity %s of project %s error", name, project.getProjectName()), e);
            }
        }
        return this.featureEntityMap.get(name);
    }

    /**
     * featureEntity 数量少，miss 时一次性分页拉全并缓存。
     */
    private void loadFeatureEntities() throws Exception {
        synchronized (loadLock) {
            if (this.featureEntitiesLoaded) {
                return;
            }
            int pageNumber = 1;
            int pageSize = 100;
            do {
                ListFeatureEntitiesResponse listFeatureEntitiesResponse = this.apiClient.getFeatureEntityApi().listFeatureEntities(String.valueOf(this.project.getProjectId()), pageNumber, pageSize);

                for (com.aliyun.openservices.paifeaturestore.model.FeatureEntity featureEntity : listFeatureEntitiesResponse.getFeatureEntities()) {
                    // projectId 是 Long，== 比的是引用，超出 Long 缓存范围后恒为 false，必须用 equals
                    if (featureEntity.getProjectId() != null && featureEntity.getProjectId().equals(project.getProjectId())) {
                        this.featureEntityMap.putIfAbsent(featureEntity.getFeatureEntityName(),
                                new com.aliyun.openservices.paifeaturestore.domain.FeatureEntity(featureEntity));
                    }
                }
                if (listFeatureEntitiesResponse.getFeatureEntities().size() == 0 || pageNumber * pageSize > listFeatureEntitiesResponse.getTotalCount()) {
                    break;
                }
                pageNumber++;
            } while (true);
            this.featureEntitiesLoaded = true;
        }
    }

    public Model getModel(String name) {
        if (!this.modelMap.containsKey(name)) {
            try {
                this.loadModelFeature(name);
            } catch (Exception e) {
                logger.error("load modelFeature {} error", name, e);
                throw new IllegalStateException(String.format("load model feature %s of project %s error", name, project.getProjectName()), e);
            }
        }
        return this.modelMap.get(name);
    }

    /**
     * 按名字从 FS server 拉取单个 model 并缓存到内存。
     */
    private void loadModelFeature(String name) throws Exception {
        synchronized (loadLock) {
            if (this.modelMap.containsKey(name)) {
                return;
            }
            int pageNumber = 1;
            int pageSize = 100;
            do {
                ListModesResponse listModesResponse = this.apiClient.getFsModelApi().listModelsByName(name, String.valueOf(project.getProjectId()), pageNumber, pageSize);
                for (com.aliyun.openservices.paifeaturestore.model.Model m : listModesResponse.getModels()) {
                    com.aliyun.openservices.paifeaturestore.model.Model model = this.apiClient.getFsModelApi().getModelById(String.valueOf(m.getModelId()));
                    com.aliyun.openservices.paifeaturestore.domain.Model domianModel = new com.aliyun.openservices.paifeaturestore.domain.Model(model, this);
                    this.addModel(model.getName(), domianModel);
                }
                if (listModesResponse.getModels().size() == 0 || pageNumber * pageSize > listModesResponse.getTotalCount()) {
                    break;
                }
                pageNumber++;
            } while (true);
        }
    }

    public Model getModelFeature(String name) {
        return this.getModel(name);
    }

    public com.aliyun.openservices.paifeaturestore.model.Project getProject() {
        return project;
    }

    public OnlineStore getOnlineStore() {
        return onlineStore;
    }

    public Map<String, IFeatureView> getFeatureViewMap() {
        return featureViewMap;
    }



    public Map<String, FeatureEntity> getFeatureEntityMap() {
        return featureEntityMap;
    }

    public Map<String, Model> getModelMap() {
        return modelMap;
    }

    public void addFeatureEntity(String featureEntityName, FeatureEntity featureEntity) {
        this.featureEntityMap.put(featureEntityName, featureEntity);
    }

    public void addFeatureView(String name, IFeatureView domainFeatureView) {
        this.featureViewMap.put(name, domainFeatureView);

    }

    public void addModel(String name, Model domianModel) {
        this.modelMap.put(name, domianModel);
    }


    public void registerFeatrueDB(Datasource featureDBDataSource) {
        if (null != featureDBDataSource) {
            this.featureDBDatasource = featureDBDataSource;
            if (null == FeatureDBFactory.get(featureDBDataSource.getName())) {
                FeatureDBClient featureDBClient = featureDBDataSource.generateFeatureDBClient(usePublicAddress);
                featureDBClient.setSignature(this.signature);
                FeatureDBFactory.register(featureDBDataSource.getName(), featureDBClient);
            }

        }
    }
    public String getFeatureDBName() {
        if (null != this.featureDBDatasource) {
            return this.featureDBDatasource.getName();
        }

        return this.onlineStore.getDatasourceName();
    }

    public void setApiClient(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    public ApiClient getApiClient() {
        return apiClient;
    }
}
