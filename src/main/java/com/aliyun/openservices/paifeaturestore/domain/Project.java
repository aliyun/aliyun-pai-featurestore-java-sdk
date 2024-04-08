package com.aliyun.openservices.paifeaturestore.domain;

import com.aliyun.openservices.paifeaturestore.constants.DatasourceType;
import com.aliyun.openservices.paifeaturestore.datasource.FeatureDBClient;
import com.aliyun.openservices.paifeaturestore.datasource.FeatureDBFactory;
import com.aliyun.openservices.paifeaturestore.datasource.Hologres;
import com.aliyun.openservices.paifeaturestore.datasource.HologresFactory;
import com.aliyun.openservices.paifeaturestore.datasource.IGraphFactory;
import com.aliyun.openservices.paifeaturestore.datasource.TableStoreFactory;
import com.aliyun.openservices.paifeaturestore.model.Datasource;
import com.aliyun.tea.utils.StringUtils;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class Project {
    com.aliyun.openservices.paifeaturestore.model.Project project;

    private final OnlineStore onlineStore;

    private final Map<String, IFeatureView> featureViewMap = new HashMap<>();

    private final Map<String, FeatureEntity> featureEntityMap = new HashMap<>();

    private final Map<String, Model> modelMap = new HashMap<>();

    private boolean usePublicAddress = false;

    private String signature = null;

    private Datasource featureDBDatasource = null;

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
        IFeatureView featureView =  this.featureViewMap.get(name);
        if (featureView instanceof FeatureView) {
            return (FeatureView) featureView;
        }
        return null;
    }

    public SequenceFeatureView getSeqFeatureView(String name) {
        IFeatureView featureView = this.featureViewMap.get(name);
        if (featureView instanceof SequenceFeatureView) {
            return (SequenceFeatureView) featureView;
        }
        return null;
    }

    public FeatureEntity getFeatureEntity(String name) {
        return this.featureEntityMap.get(name);
    }

    public Model getModel(String name) {
        return this.modelMap.get(name);
    }

    public Model getModelFeature(String name) {
        return this.modelMap.get(name);
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
}
