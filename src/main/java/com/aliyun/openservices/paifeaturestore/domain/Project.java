package com.aliyun.openservices.paifeaturestore.domain;

import com.aliyun.openservices.paifeaturestore.constants.DatasourceType;
import com.aliyun.openservices.paifeaturestore.datasource.Hologres;
import com.aliyun.openservices.paifeaturestore.datasource.HologresFactory;
import com.aliyun.openservices.paifeaturestore.datasource.IGraphFactory;
import com.aliyun.openservices.paifeaturestore.datasource.TableStoreFactory;

import java.util.HashMap;
import java.util.Map;

public class Project {
    com.aliyun.openservices.paifeaturestore.model.Project project;

    private final OnlineStore onlineStore;

    private final Map<String, FeatureView> featureViewMap = new HashMap<>();

    private final Map<String, FeatureEntity> featureEntityMap = new HashMap<>();

    private final Map<String, Model> modelMap = new HashMap<>();

    public Project(com.aliyun.openservices.paifeaturestore.model.Project project) throws Exception {
        this.project = project;

        switch (project.getOnlineDatasourceType()) {
            case Datasource_Type_Hologres:
                HologresOnlinestore hologresOnlinestore = new HologresOnlinestore();
                hologresOnlinestore.setDatasource(project.getOnlineDataSource());
                this.onlineStore = hologresOnlinestore;
                if (null == HologresFactory.get(this.onlineStore.getDatasourceName())) {
                    Hologres hologres = new Hologres(hologresOnlinestore.getDatasource().generateDSN(DatasourceType.Datasource_Type_Hologres));
                    HologresFactory.register(this.onlineStore.getDatasourceName(), hologres);
                }
                break;
            case Datasource_Type_IGraph:
                IGraphOnlineStore iGraphOnlineStore = new IGraphOnlineStore();
                iGraphOnlineStore.setDatasource(project.getOnlineDataSource());
                this.onlineStore = iGraphOnlineStore;
                if (null == IGraphFactory.get(this.onlineStore.getDatasourceName())) {
                    IGraphFactory.register(this.onlineStore.getDatasourceName(),
                            iGraphOnlineStore.getDatasource().generateIgraphClient());
                }
                break;
            case Datasource_Type_TableStore:
                TableStoreOnlinestore tableStoreOnlinestore = new TableStoreOnlinestore();
                tableStoreOnlinestore.setDatasource(project.getOnlineDataSource());
                this.onlineStore = tableStoreOnlinestore;
                if (null == TableStoreFactory.get(this.onlineStore.getDatasourceName())) {
                    TableStoreFactory.register(this.onlineStore.getDatasourceName(),
                            tableStoreOnlinestore.getDatasource().generateOTSClient());
                }
                break;
            default:
                throw new RuntimeException("not support onlinestore type");
        }
    }

    public FeatureView getFeatureView(String name) {
        return this.featureViewMap.get(name);
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

    public Map<String, FeatureView> getFeatureViewMap() {
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

    public void addFeatureView(String name, FeatureView domainFeatureView) {
        this.featureViewMap.put(name, domainFeatureView);
    }

    public void addModel(String name, Model domianModel) {
        this.modelMap.put(name, domianModel);
    }
}
