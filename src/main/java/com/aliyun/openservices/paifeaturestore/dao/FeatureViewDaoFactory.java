package com.aliyun.openservices.paifeaturestore.dao;
/* This class creates specific feature view implementation classes based on the data source types configured with daoconfig.*/
public class FeatureViewDaoFactory {

    public static FeatureViewDao getFeatureViewDao(DaoConfig daoConfig) {
        switch (daoConfig.datasourceType) {
            case Datasource_Type_IGraph:
                return new FeatureViewIgraphDao(daoConfig);
            case Datasource_Type_Hologres:
                return new FeatureViewHologresDao(daoConfig);
            case Datasource_Type_TableStore:
                return new FeatureViewTableStoreDao(daoConfig);
            case Datasource_Type_FeatureDB:
                return new FeatureViewFeatureDBDao(daoConfig);
        }

        throw new RuntimeException("not found FeatureViewDao implement");
    }
}
