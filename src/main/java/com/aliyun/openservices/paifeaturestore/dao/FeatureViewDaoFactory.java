package com.aliyun.openservices.paifeaturestore.dao;

public class FeatureViewDaoFactory {

    public static FeatureViewDao getFeatureViewDao(DaoConfig daoConfig) {
        switch (daoConfig.datasourceType) {
            case Datasource_Type_IGraph:
                return new FeatureViewIgraphDao(daoConfig);
            case Datasource_Type_Hologres:
                return new FeatureViewHologresDao(daoConfig);
            case Datasource_Type_TableStore:
                return new FeatureViewTableStoreDao(daoConfig);
        }

        throw new RuntimeException("not found FeatureViewDao implement");
    }
}
