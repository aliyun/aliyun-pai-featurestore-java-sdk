package com.aliyun.openservices.paifeaturestore.model;

import java.util.Objects;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;
/**
 * FeatureView
 */

public class FeatureView {
  @SerializedName("feature_view_id")
  private Long featureViewId = null;

  @SerializedName("project_id")
  private Long projectId = null;

  @SerializedName("project_name")
  private String projectName = null;

  @SerializedName("name")
  private String name = null;

  @SerializedName("feature_entity_id")
  private Integer featureEntityId = null;

  @SerializedName("feature_entity_name")
  private String featureEntityName = null;

  @SerializedName("feature_entity_joinid")
  private String featureEntityJoinid = null;

  @SerializedName("owner")
  private String owner = null;

  @SerializedName("type")
  private String type = null;

  @SerializedName("online")
  private Boolean online = null;

  @SerializedName("offline")
  private Boolean offline = null;

  @SerializedName("is_register")
  private Boolean isRegister = null;

  @SerializedName("register_table")
  private String registerTable = null;

  @SerializedName("register_datasource_id")
  private Integer registerDatasourceId = 0;

  @SerializedName("register_datasource_name")
  private String registerDatasourceName = null;

  @SerializedName("ttl")
  private Integer ttl = null;

  @SerializedName("tags")
  private List<String> tags = new ArrayList<String>();

  @SerializedName("config")
  private String config = null;

  @SerializedName("online_table_name")
  private String onlineTableName = null;

  @SerializedName("offline_table_name")
  private String offlineTableName = null;

  @SerializedName("last_sync_time")
  private String lastSyncTime = null;

  @SerializedName("last_sync_config")
  private String lastSyncConfig = null;

  @SerializedName("fields")
  private List<FeatureViewRequestFields> fields = new ArrayList<FeatureViewRequestFields>();

  private Datasource registerDatasource;

  public Datasource getRegisterDatasource() {
    return registerDatasource;
  }

  public void setRegisterDatasource(Datasource registerDatasource) {
    this.registerDatasource = registerDatasource;
  }

  public FeatureView featureViewId(Long featureViewId) {
    this.featureViewId = featureViewId;
    return this;
  }

   /**
   * Get featureViewId
   * @return featureViewId
  **/
  public Long getFeatureViewId() {
    return featureViewId;
  }

  public void setFeatureViewId(Long featureViewId) {
    this.featureViewId = featureViewId;
  }

  public FeatureView projectId(Long projectId) {
    this.projectId = projectId;
    return this;
  }

   /**
   * Get projectId
   * @return projectId
  **/
  public Long getProjectId() {
    return projectId;
  }

  public void setProjectId(Long projectId) {
    this.projectId = projectId;
  }

  public FeatureView projectName(String projectName) {
    this.projectName = projectName;
    return this;
  }

   /**
   * Get projectName
   * @return projectName
  **/
  public String getProjectName() {
    return projectName;
  }

  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }

  public FeatureView name(String name) {
    this.name = name;
    return this;
  }

   /**
   * Get name
   * @return name
  **/
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public FeatureView featureEntityId(Integer featureEntityId) {
    this.featureEntityId = featureEntityId;
    return this;
  }

   /**
   * Get featureEntityId
   * @return featureEntityId
  **/
  public Integer getFeatureEntityId() {
    return featureEntityId;
  }

  public void setFeatureEntityId(Integer featureEntityId) {
    this.featureEntityId = featureEntityId;
  }

  public FeatureView featureEntityName(String featureEntityName) {
    this.featureEntityName = featureEntityName;
    return this;
  }

   /**
   * Get featureEntityName
   * @return featureEntityName
  **/
  public String getFeatureEntityName() {
    return featureEntityName;
  }

  public void setFeatureEntityName(String featureEntityName) {
    this.featureEntityName = featureEntityName;
  }

  public FeatureView featureEntityJoinid(String featureEntityJoinid) {
    this.featureEntityJoinid = featureEntityJoinid;
    return this;
  }

   /**
   * Get featureEntityJoinid
   * @return featureEntityJoinid
  **/
  public String getFeatureEntityJoinid() {
    return featureEntityJoinid;
  }

  public void setFeatureEntityJoinid(String featureEntityJoinid) {
    this.featureEntityJoinid = featureEntityJoinid;
  }

  public FeatureView owner(String owner) {
    this.owner = owner;
    return this;
  }

   /**
   * Get owner
   * @return owner
  **/
  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public FeatureView type(String type) {
    this.type = type;
    return this;
  }

   /**
   * Get type
   * @return type
  **/
  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public FeatureView online(Boolean online) {
    this.online = online;
    return this;
  }

   /**
   * Get online
   * @return online
  **/
  public Boolean isOnline() {
    return online;
  }

  public void setOnline(Boolean online) {
    this.online = online;
  }

  public FeatureView offline(Boolean offline) {
    this.offline = offline;
    return this;
  }

   /**
   * Get offline
   * @return offline
  **/
  public Boolean isOffline() {
    return offline;
  }

  public void setOffline(Boolean offline) {
    this.offline = offline;
  }

  public FeatureView isRegister(Boolean isRegister) {
    this.isRegister = isRegister;
    return this;
  }

   /**
   * Get isRegister
   * @return isRegister
  **/
  public Boolean isIsRegister() {
    return isRegister;
  }

  public void setIsRegister(Boolean isRegister) {
    this.isRegister = isRegister;
  }

  public FeatureView registerTable(String registerTable) {
    this.registerTable = registerTable;
    return this;
  }

   /**
   * Get registerTable
   * @return registerTable
  **/
  public String getRegisterTable() {
    return registerTable;
  }

  public void setRegisterTable(String registerTable) {
    this.registerTable = registerTable;
  }

  public FeatureView registerDatasourceId(Integer registerDatasourceId) {
    this.registerDatasourceId = registerDatasourceId;
    return this;
  }

   /**
   * Get registerDatasourceId
   * @return registerDatasourceId
  **/
  public Integer getRegisterDatasourceId() {
    return registerDatasourceId;
  }

  public void setRegisterDatasourceId(Integer registerDatasourceId) {
    this.registerDatasourceId = registerDatasourceId;
  }

  public FeatureView registerDatasourceName(String registerDatasourceName) {
    this.registerDatasourceName = registerDatasourceName;
    return this;
  }

   /**
   * Get registerDatasourceName
   * @return registerDatasourceName
  **/
  public String getRegisterDatasourceName() {
    return registerDatasourceName;
  }

  public void setRegisterDatasourceName(String registerDatasourceName) {
    this.registerDatasourceName = registerDatasourceName;
  }

  public FeatureView ttl(Integer ttl) {
    this.ttl = ttl;
    return this;
  }

   /**
   * Get ttl
   * @return ttl
  **/
  public Integer getTtl() {
    return ttl;
  }

  public void setTtl(Integer ttl) {
    this.ttl = ttl;
  }

  public FeatureView tags(List<String> tags) {
    this.tags = tags;
    return this;
  }

  public FeatureView addTagsItem(String tagsItem) {
    this.tags.add(tagsItem);
    return this;
  }

   /**
   * Get tags
   * @return tags
  **/
  public List<String> getTags() {
    return tags;
  }

  public void setTags(List<String> tags) {
    this.tags = tags;
  }

  public FeatureView config(String config) {
    this.config = config;
    return this;
  }

   /**
   * Get config
   * @return config
  **/
  public String getConfig() {
    return config;
  }

  public void setConfig(String config) {
    this.config = config;
  }

  public FeatureView onlineTableName(String onlineTableName) {
    this.onlineTableName = onlineTableName;
    return this;
  }

   /**
   * Get onlineTableName
   * @return onlineTableName
  **/
  public String getOnlineTableName() {
    return onlineTableName;
  }

  public void setOnlineTableName(String onlineTableName) {
    this.onlineTableName = onlineTableName;
  }

  public FeatureView offlineTableName(String offlineTableName) {
    this.offlineTableName = offlineTableName;
    return this;
  }

   /**
   * Get offlineTableName
   * @return offlineTableName
  **/
  public String getOfflineTableName() {
    return offlineTableName;
  }

  public void setOfflineTableName(String offlineTableName) {
    this.offlineTableName = offlineTableName;
  }

  public FeatureView lastSyncTime(String lastSyncTime) {
    this.lastSyncTime = lastSyncTime;
    return this;
  }

   /**
   * Get lastSyncTime
   * @return lastSyncTime
  **/
  public String getLastSyncTime() {
    return lastSyncTime;
  }

  public void setLastSyncTime(String lastSyncTime) {
    this.lastSyncTime = lastSyncTime;
  }

  public FeatureView lastSyncConfig(String lastSyncConfig) {
    this.lastSyncConfig = lastSyncConfig;
    return this;
  }

   /**
   * Get lastSyncConfig
   * @return lastSyncConfig
  **/
  public String getLastSyncConfig() {
    return lastSyncConfig;
  }

  public void setLastSyncConfig(String lastSyncConfig) {
    this.lastSyncConfig = lastSyncConfig;
  }

  public FeatureView fields(List<FeatureViewRequestFields> fields) {
    this.fields = fields;
    return this;
  }

  public FeatureView addFieldsItem(FeatureViewRequestFields fieldsItem) {
    this.fields.add(fieldsItem);
    return this;
  }

   /**
   * Get fields
   * @return fields
  **/
  public List<FeatureViewRequestFields> getFields() {
    return fields;
  }

  public void setFields(List<FeatureViewRequestFields> fields) {
    this.fields = fields;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FeatureView featureView = (FeatureView) o;
    return Objects.equals(this.featureViewId, featureView.featureViewId) &&
        Objects.equals(this.projectId, featureView.projectId) &&
        Objects.equals(this.projectName, featureView.projectName) &&
        Objects.equals(this.name, featureView.name) &&
        Objects.equals(this.featureEntityId, featureView.featureEntityId) &&
        Objects.equals(this.featureEntityName, featureView.featureEntityName) &&
        Objects.equals(this.featureEntityJoinid, featureView.featureEntityJoinid) &&
        Objects.equals(this.owner, featureView.owner) &&
        Objects.equals(this.type, featureView.type) &&
        Objects.equals(this.online, featureView.online) &&
        Objects.equals(this.offline, featureView.offline) &&
        Objects.equals(this.isRegister, featureView.isRegister) &&
        Objects.equals(this.registerTable, featureView.registerTable) &&
        Objects.equals(this.registerDatasourceId, featureView.registerDatasourceId) &&
        Objects.equals(this.registerDatasourceName, featureView.registerDatasourceName) &&
        Objects.equals(this.ttl, featureView.ttl) &&
        Objects.equals(this.tags, featureView.tags) &&
        Objects.equals(this.config, featureView.config) &&
        Objects.equals(this.onlineTableName, featureView.onlineTableName) &&
        Objects.equals(this.offlineTableName, featureView.offlineTableName) &&
        Objects.equals(this.lastSyncTime, featureView.lastSyncTime) &&
        Objects.equals(this.lastSyncConfig, featureView.lastSyncConfig) &&
        Objects.equals(this.fields, featureView.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(featureViewId, projectId, projectName, name, featureEntityId, featureEntityName, featureEntityJoinid, owner, type, online, offline, isRegister, registerTable, registerDatasourceId, registerDatasourceName, ttl, tags, config, onlineTableName, offlineTableName, lastSyncTime, lastSyncConfig, fields);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class FeatureView {\n");
    
    sb.append("    featureViewId: ").append(toIndentedString(featureViewId)).append("\n");
    sb.append("    projectId: ").append(toIndentedString(projectId)).append("\n");
    sb.append("    projectName: ").append(toIndentedString(projectName)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    featureEntityId: ").append(toIndentedString(featureEntityId)).append("\n");
    sb.append("    featureEntityName: ").append(toIndentedString(featureEntityName)).append("\n");
    sb.append("    featureEntityJoinid: ").append(toIndentedString(featureEntityJoinid)).append("\n");
    sb.append("    owner: ").append(toIndentedString(owner)).append("\n");
    sb.append("    type: ").append(toIndentedString(type)).append("\n");
    sb.append("    online: ").append(toIndentedString(online)).append("\n");
    sb.append("    offline: ").append(toIndentedString(offline)).append("\n");
    sb.append("    isRegister: ").append(toIndentedString(isRegister)).append("\n");
    sb.append("    registerTable: ").append(toIndentedString(registerTable)).append("\n");
    sb.append("    registerDatasourceId: ").append(toIndentedString(registerDatasourceId)).append("\n");
    sb.append("    registerDatasourceName: ").append(toIndentedString(registerDatasourceName)).append("\n");
    sb.append("    ttl: ").append(toIndentedString(ttl)).append("\n");
    sb.append("    tags: ").append(toIndentedString(tags)).append("\n");
    sb.append("    config: ").append(toIndentedString(config)).append("\n");
    sb.append("    onlineTableName: ").append(toIndentedString(onlineTableName)).append("\n");
    sb.append("    offlineTableName: ").append(toIndentedString(offlineTableName)).append("\n");
    sb.append("    lastSyncTime: ").append(toIndentedString(lastSyncTime)).append("\n");
    sb.append("    lastSyncConfig: ").append(toIndentedString(lastSyncConfig)).append("\n");
    sb.append("    fields: ").append(toIndentedString(fields)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

}
