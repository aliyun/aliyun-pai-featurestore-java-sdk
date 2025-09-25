package com.aliyun.openservices.paifeaturestore.domain;

import com.aliyun.openservices.paifeaturestore.constants.DatasourceType;
import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.aliyun.openservices.paifeaturestore.constants.InsertMode;
import com.aliyun.openservices.paifeaturestore.dao.DaoConfig;
import com.aliyun.openservices.paifeaturestore.dao.FeatureViewDao;
import com.aliyun.openservices.paifeaturestore.dao.FeatureViewDaoFactory;
import com.aliyun.openservices.paifeaturestore.model.FeatureViewRequestFields;
import com.aliyun.tea.utils.StringUtils;
import com.google.gson.Gson;
import org.jacoco.agent.rt.internal_035b120.core.internal.flow.IFrame;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FeatureView implements IFeatureView {
    com.aliyun.openservices.paifeaturestore.model.FeatureView featureView;

    private final Project project;

    private final FeatureEntity featureEntity;

    private final FeatureViewDao featureViewDao;

    private FeatureViewRequestFields primaryKeyField;

    private FeatureViewRequestFields eventTimeField;

    private final List<String> featureFields = new ArrayList<>();

    public FeatureView(com.aliyun.openservices.paifeaturestore.model.FeatureView featureView, Project project, FeatureEntity featureEntity) {
        this.featureView = featureView;
        this.project = project;
        this.featureEntity = featureEntity;

        for (FeatureViewRequestFields field : this.featureView.getFields()) {
            if (field.isIsEventTime()) {
                eventTimeField = field;
                this.featureFields.add(field.getName());
            } else if (field.isIsPrimaryKey()) {
                primaryKeyField = field;
            } else if (field.isIsPartition()) {
                continue;
            } else {
                this.featureFields.add(field.getName());
            }
        }

        DaoConfig daoConfig = new DaoConfig();
        daoConfig.datasourceType = project.getProject().getOnlineDatasourceType();
        daoConfig.primaryKeyField = this.primaryKeyField.getName();
        if (null != this.eventTimeField) {
            daoConfig.eventTimeField = this.eventTimeField.getName();
        }
        daoConfig.ttl = featureView.getTtl();

        Map<String, FSType> fieldTypeMap2 = new HashMap<>();
        for (FeatureViewRequestFields field : featureView.getFields()) {
            if (field.isIsPrimaryKey()) {
                fieldTypeMap2.put(field.getName(), field.getType());
            } else if (field.isIsPartition()) {
                continue;
            } else {
                fieldTypeMap2.put(field.getName(), field.getType());
            }
        }
        daoConfig.fieldTypeMap = fieldTypeMap2;

        if ((null != featureView.getWriteToFeaturedb() && featureView.getWriteToFeaturedb())  || project.getProject().getOnlineDatasourceType().equals(DatasourceType.Datasource_Type_FeatureDB)) {
            daoConfig.datasourceType = DatasourceType.Datasource_Type_FeatureDB;
            daoConfig.featureDBName = project.getFeatureDBName();
            daoConfig.featureDBDatabase = project.getProject().getInstanceId();
            daoConfig.featureDBSchema = project.getProject().getProjectName();
            daoConfig.featureDBTable = featureView.getName();
            daoConfig.fields = this.featureFields;
        } else {
            switch (project.getProject().getOnlineDatasourceType()) {
                case Datasource_Type_Hologres:
                    daoConfig.hologresName = project.getOnlineStore().getDatasourceName();
                    daoConfig.hologresTableName = project.getOnlineStore().getTableName(this);
                    break;
                case Datasource_Type_IGraph:
                    if (!StringUtils.isEmpty(featureView.getConfig())) {
                        Gson gson = new Gson();
                        Map map = gson.fromJson(featureView.getConfig(), Map.class);
                        if (map.containsKey("save_original_field")) {
                            if (map.get("save_original_field") instanceof Boolean) {
                                daoConfig.saveOriginalField = (Boolean) map.get("save_original_field");
                            }
                        }
                    }

                    daoConfig.iGraphName = project.getOnlineStore().getDatasourceName();
                    daoConfig.groupName = project.getProject().getProjectName();
                    daoConfig.labelName = project.getOnlineStore().getTableName(this);

                    Map<String, String> fieldMap = new HashMap<>();
                    Map<String, FSType> fieldTypeMap = new HashMap<>();
                    for (FeatureViewRequestFields field : featureView.getFields()) {
                        if (field.isIsPrimaryKey()) {
                            fieldMap.put(field.getName(), field.getName());
                            fieldTypeMap.put(field.getName(), field.getType());
                        } else if (field.isIsPartition()) {
                            continue;
                        } else {
                            String name;
                            if (daoConfig.saveOriginalField) {
                                name = field.getName();
                            } else {
                                name = String.format("f%d", field.getPosition());
                            }

                            fieldMap.put(name, field.getName());
                            fieldTypeMap.put(name, field.getType());
                        }
                    }

                    daoConfig.fieldMap = fieldMap;
                    daoConfig.fieldTypeMap = fieldTypeMap;
                    break;
                case Datasource_Type_TableStore:
                    daoConfig.otsTableName = project.getOnlineStore().getTableName(this);
                    daoConfig.otsName = project.getOnlineStore().getDatasourceName();
                    break;
                default:
                    break;
            }

        }

        this.featureViewDao = FeatureViewDaoFactory.getFeatureViewDao(daoConfig);
    }


    public com.aliyun.openservices.paifeaturestore.model.FeatureView getFeatureView() {
        return featureView;
    }

    public Project getProject() {
        return project;
    }

    public void setFeatureView(com.aliyun.openservices.paifeaturestore.model.FeatureView featureView) {
        this.featureView = featureView;
    }

    public FeatureEntity getFeatureEntity() {
        return featureEntity;
    }

    @Override
    public void writeFeatures(List<Map<String, Object>> data) {
        this.writeFeatures(data, InsertMode.FullRowWrite);
    }

    @Override
    public void writeFeatures(List<Map<String, Object>> data, InsertMode insertMode) {
        for (Map<String, Object> record : data) {
            record.put("__insert_mode__", insertMode);
        }

        this.featureViewDao.writeFeatures(data);
    }


    public FeatureResult getOnlineFeatures(String[] joinIds) throws Exception {
        return this.getOnlineFeatures(joinIds, new String[]{"*"}, null);
    }

    public FeatureResult getOnlineFeatures(String[] joinIds, String[] features, Map<String, String> aliasFields) throws Exception {
        List<String> selectFields = new ArrayList<>();
        selectFields.add(this.primaryKeyField.getName());
        for (String featureName : features) {
            if ("*".equals(featureName)) {
                selectFields.addAll(this.featureFields);
            } else {
                if (!this.featureFields.contains(featureName)) {
                    throw new RuntimeException(String.format("feature name :%s not found in the featureview fields", featureName));
                }

                selectFields.add(featureName);
            }
        }

        if (null != aliasFields) {
            for (String featureName : aliasFields.keySet()) {
                if (!this.featureFields.contains(featureName)) {
                    throw new RuntimeException(String.format("alias fields feature name :%s not found in the featureview fields", featureName));
                }
            }
        }

        FeatureStoreResult featureStoreResult = (FeatureStoreResult) this.featureViewDao.getFeatures(joinIds, selectFields.toArray(new String[0]));

        String[] featureFields = featureStoreResult.getFeatureFields();
        List<String> featureFieldList = new ArrayList<>(Arrays.asList(featureFields));
        Map<String, FSType> fieldTypeMap = featureStoreResult.getFeatureFieldTypeMap();

        if (!this.primaryKeyField.getName().equals(this.featureEntity.getFeatureEntity().getFeatureEntityJoinid())) {
            for (Map<String, Object> featureMap : featureStoreResult.getFeatureData()) {
                featureMap.put(this.featureEntity.getFeatureEntity().getFeatureEntityJoinid(), featureMap.get(this.primaryKeyField.getName()));
                featureMap.remove(this.primaryKeyField.getName());
                featureFieldList.add(this.featureEntity.getFeatureEntity().getFeatureEntityJoinid());
                featureFieldList.remove(this.primaryKeyField.getName());

                fieldTypeMap.put(this.featureEntity.getFeatureEntity().getFeatureEntityJoinid(), fieldTypeMap.get(this.primaryKeyField.getName()));
            }
        }

        if (null != aliasFields) {
            for (Map.Entry<String, String> entry : aliasFields.entrySet()) {
                for (Map<String, Object> featureMap : featureStoreResult.getFeatureData()) {
                    if (featureMap.containsKey(entry.getKey())) {
                        featureMap.put(entry.getValue(), featureMap.get(entry.getKey()));
                        featureMap.remove(entry.getKey());
                    }
                }
                featureFieldList.add(entry.getValue());
                featureFieldList.remove(entry.getKey());
                fieldTypeMap.put(entry.getValue(), fieldTypeMap.get(entry.getKey()));
            }
        }

        featureStoreResult.setFeatureFields(featureFieldList.toArray(new String[0]));

        return featureStoreResult;
    }

    @Override
    public String getName() {
        return featureView.getName();
    }

    @Override
    public String getFeatureEntityName() {
        return featureView.getFeatureEntityName();
    }

    @Override
    public String getType() {
        return featureView.getType();
    }


    @Override
    public String toString() {
        return "FeatureView{" +
                "featureView=" + featureView +
                '}';
    }
}
