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

import java.util.HashMap;
import java.util.Map;

public class Project {
    public static Logger logger = LoggerFactory.getLogger(Project.class);
    com.aliyun.openservices.paifeaturestore.model.Project project;

    private final OnlineStore onlineStore;

    private final Map<String, IFeatureView> featureViewMap = new HashMap<>();

    private final Map<String, FeatureEntity> featureEntityMap = new HashMap<>();

    private final Map<String, Model> modelMap = new HashMap<>();

    private boolean usePublicAddress = false;

    private String signature = null;

    private Datasource featureDBDatasource = null;
    private ApiClient apiClient;

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

        //

    }


    public void setUsePublicAddress(boolean usePublicAddress) {
        this.usePublicAddress = usePublicAddress;
    }

    public FeatureView getFeatureView(String name) {
        if (!this.featureViewMap.containsKey(name)) {
            try {
                this.loadFeatureView(name);
            } catch (Exception e) {
                logger.error("load feature view error", e);
            }
        }
        IFeatureView featureView =  this.featureViewMap.get(name);
        if (featureView instanceof FeatureView) {
            return (FeatureView) featureView;
        }
        return null;
    }

    private void loadFeatureView(String name) throws Exception {
        int pageNumber = 1;
        int pageSize = 100;
        do {

            ListFeatureViewsResponse listFeatureViewsResponse =  this.apiClient.getFeatureViewApi().listFeatureViewsByName(name, String.valueOf(project.getProjectId()), pageNumber, pageSize);
            for (com.aliyun.openservices.paifeaturestore.model.FeatureView view: listFeatureViewsResponse.getFeatureViews()) {

                com.aliyun.openservices.paifeaturestore.model.FeatureView featureView = this.apiClient.getFeatureViewApi().getFeatureViewById(String.valueOf(view.getFeatureViewId()));
                if (featureView.getRegisterDatasourceId() > 0) {
                    Datasource registerDatasource = this.apiClient.getDatasourceApi().getDatasourceById(featureView.getRegisterDatasourceId());
                    featureView.setRegisterDatasource(registerDatasource);
                }

                IFeatureView domainFeatureView = FeatureViewFactory.getFeatureView(featureView, this, this.getFeatureEntity(featureView.getFeatureEntityName()) );

                this.addFeatureView(featureView.getName(), domainFeatureView);
            }


            if (listFeatureViewsResponse.getFeatureViews().size() == 0 || pageNumber * pageSize > listFeatureViewsResponse.getTotalCount()) {
                break;
            }

            pageNumber++;
        } while (true);
    }

    public SequenceFeatureView getSeqFeatureView(String name) {
        if (!this.featureViewMap.containsKey(name)) {
            try {
                this.loadFeatureView(name);
            } catch (Exception e) {
                logger.error("load feature view error", e);
            }
        }
        IFeatureView featureView = this.featureViewMap.get(name);
        if (featureView instanceof SequenceFeatureView) {
            return (SequenceFeatureView) featureView;
        }
        return null;
    }

    public FeatureEntity getFeatureEntity(String name) {

        if (!this.featureEntityMap.containsKey(name)) {
            try {
                this.loadFeatureEntities();
            } catch (Exception e) {
                logger.error("load feature entity error", e);
            }
        }
        return this.featureEntityMap.get(name);
    }

    private void loadFeatureEntities() throws Exception {
        int pageNumber = 1;
        int pageSize = 100;
        do {
            ListFeatureEntitiesResponse listFeatureEntitiesResponse = this.apiClient.getFeatureEntityApi().listFeatureEntities(String.valueOf(this.project.getProjectId()), pageNumber, pageSize);

            for (com.aliyun.openservices.paifeaturestore.model.FeatureEntity featureEntity : listFeatureEntitiesResponse.getFeatureEntities()) {
                if (featureEntity.getProjectId() == project.getProjectId()) {
                    if (!this.featureEntityMap.containsKey(featureEntity.getFeatureEntityName()))  {
                        this.featureEntityMap.put(featureEntity.getFeatureEntityName(), new com.aliyun.openservices.paifeaturestore.domain.FeatureEntity(featureEntity));
                    }
                }
            }
            if (listFeatureEntitiesResponse.getFeatureEntities().size() == 0 || pageNumber * pageSize > listFeatureEntitiesResponse.getTotalCount()) {
                break;
            }
            pageNumber++;
        } while (true);
    }

    public Model getModel(String name) {
        if (!this.modelMap.containsKey(name)) {
            try {
                this.loadModelFeature(name);
            } catch (Exception e) {
                logger.error("load modelFeature error", e);
            }
        }
        return this.modelMap.get(name);
    }

    private void loadModelFeature(String name) throws Exception {
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

    public void close() throws Exception {
        for (Model model : this.modelMap.values()) {
            model.close();
        }
        this.modelMap.clear();

        for (IFeatureView featureView : this.featureViewMap.values()) {
            featureView.close();
        }
        this.featureViewMap.clear();
        this.featureEntityMap.clear();
    }
}
