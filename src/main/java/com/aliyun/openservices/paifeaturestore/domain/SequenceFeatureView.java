package com.aliyun.openservices.paifeaturestore.domain;

import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.aliyun.openservices.paifeaturestore.dao.DaoConfig;
import com.aliyun.openservices.paifeaturestore.dao.FeatureViewDao;
import com.aliyun.openservices.paifeaturestore.dao.FeatureViewDaoFactory;
import com.aliyun.openservices.paifeaturestore.model.FeatureView;
import com.aliyun.openservices.paifeaturestore.model.FeatureViewRequestFields;
import com.aliyun.openservices.paifeaturestore.model.FeatureViewSeqConfig;
import com.aliyun.openservices.paifeaturestore.model.SeqConfig;
import com.aliyun.tea.utils.StringUtils;
import com.google.gson.Gson;

import java.util.*;
/* This class contains configuration information about the serialized feature view.*/
public class SequenceFeatureView implements IFeatureView{
    com.aliyun.openservices.paifeaturestore.model.FeatureView featureView;

    private final Project project;

    private final FeatureEntity featureEntity;

    private final FeatureViewDao featureViewDao;

    private String userIdField;

    private FeatureViewSeqConfig config;

    private final Map<String,String> offOnlineSeqMap = new HashMap<>();

    public SequenceFeatureView(com.aliyun.openservices.paifeaturestore.model.FeatureView featureView, Project project, FeatureEntity featureEntity) {
        this.featureView = featureView;
        this.project = project;
        this.featureEntity = featureEntity;
        Gson gson = new Gson();
        this.config=this.jsonToSeqConfig(featureView.getConfig());

        for (FeatureViewRequestFields field : this.featureView.getFields()) {
            if (field.isIsPrimaryKey()) {
                this.userIdField = field.getName();
                break;
            }
        }

        for (SeqConfig seqConfig: this.config.getSeqConfigs()) {
            offOnlineSeqMap.put(seqConfig.getOfflineSeqName(),seqConfig.getOnlineSeqName());
        }

        String[] s1={"user_id","item_id","event"};
        String[] s2={"user_id","item_id","event","timestamp"};
        int i=0;
        if (this.config.getDeduplicationMethod().length==s1.length) {
            for (String methodname:this.config.getDeduplicationMethod()) {
                if (!methodname.equals(s1[i])) {
                    throw new RuntimeException("deduplication_method invalid");
                }
                i++;
            }
            this.config.setDeduplicationMethodNum(1);
        } else if (this.config.getDeduplicationMethod().length==s2.length) {
            for (String methodname:this.config.getDeduplicationMethod()) {
                if (!methodname.equals(s2[i])) {
                    throw new RuntimeException("deduplication_method invalid");
                }
                i++;
            }
            this.config.setDeduplicationMethodNum(2);
        } else {
            throw new RuntimeException("deduplication_method invalid");
        }

        DaoConfig daoConfig = new DaoConfig();
        daoConfig.datasourceType = project.getProject().getOnlineDatasourceType();
        daoConfig.primaryKeyField=this.userIdField;

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

        if (null != featureView.getWriteToFeaturedb() &&  featureView.getWriteToFeaturedb()) {
            throw  new RuntimeException("sequence feature view not support featuredb yet");
        }
        switch (project.getProject().getOnlineDatasourceType()) {
            case Datasource_Type_Hologres:
                daoConfig.hologresName = project.getOnlineStore().getDatasourceName();
                daoConfig.hologresSeqOfflineTableName = project.getOnlineStore().getSeqOfflineTableName(this);
                daoConfig.hologresSeqOnlineTableName=project.getOnlineStore().getSeqOnlineTableName(this);
                break;
            case Datasource_Type_IGraph:
                if (!StringUtils.isEmpty(featureView.getConfig())) {
                    Map map = gson.fromJson(featureView.getConfig(), Map.class);
                    if (map.containsKey("save_original_field")) {
                        if (map.get("save_original_field") instanceof Boolean) {
                            daoConfig.saveOriginalField = (Boolean) map.get("save_original_field");
                        }
                    }
                }

                daoConfig.iGraphName = project.getOnlineStore().getDatasourceName();
                daoConfig.groupName = project.getProject().getProjectName();
                daoConfig.igraphEdgeName=project.getOnlineStore().getSeqOnlineTableName(this);
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
                daoConfig.otsName = project.getOnlineStore().getDatasourceName();
                daoConfig.otsSeqOnlineTableName=project.getOnlineStore().getSeqOnlineTableName(this);
                daoConfig.otsSeqOfflineTableName=project.getOnlineStore().getSeqOfflineTableName(this);
                break;
            case Datasource_Type_FeatureDB:
                throw  new RuntimeException("sequence feature view not support featuredb yet");
            default:
                break;
        }

        featureViewDao = FeatureViewDaoFactory.getFeatureViewDao(daoConfig);
    }

    public FeatureViewSeqConfig jsonToSeqConfig(String config){
        Gson gson = new Gson();
        FeatureViewSeqConfig featureViewSeqConfig = gson.fromJson(config, FeatureViewSeqConfig.class);
        return featureViewSeqConfig;
    }


    public Project getProject() {
        return project;
    }

    public FeatureView getFeatureView() {
        return featureView;
    }

    @Override
    public FeatureEntity getFeatureEntity() {
        return featureEntity;
    }

    @Override
    public FeatureResult getOnlineFeatures(String[] joinIds) throws Exception {
        return this.getOnlineFeatures(joinIds, new String[]{"*"}, null);
    }

    @Override
    public FeatureResult getOnlineFeatures(String[] joinIds, String[] features, Map<String, String> aliasFields) throws Exception {
        FeatureViewSeqConfig config = this.config;
        List<SeqConfig> onlineseqConfigs =new ArrayList<>();

        for (String f:features) {
            if (f.equals("*")) {
                onlineseqConfigs=Arrays.asList(config.getSeqConfigs());
                break;
            } else {
                for (SeqConfig sc:config.getSeqConfigs()) {
                    if (sc.getOnlineSeqName().equals(f)) {
                        onlineseqConfigs.add(sc);
                        break;
                    }
                }
                if (f==null) {
                    throw new RuntimeException(String.format("sequence feature name :%s not found in feature view config",f));
                }
            }
        }
        SeqConfig[] seqConfigs = new SeqConfig[onlineseqConfigs.size()];
        for (int k=0;k<onlineseqConfigs.size();k++) {
            seqConfigs[k]=onlineseqConfigs.get(k);
        }
        config.setSeqConfigs(seqConfigs);

        FeatureResult sequenceFeatures = this.featureViewDao.getSequenceFeatures(joinIds, this.userIdField, config);
        return sequenceFeatures;
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
        return this.featureView.getType();
    }


    public SeqConfig[] getSeqConfigs() {
        return this.config.getSeqConfigs();
    }
    @Override
    public String toString() {
        return "FeatureView{" +
                "featureView=" + featureView +
                '}';
    }
}
