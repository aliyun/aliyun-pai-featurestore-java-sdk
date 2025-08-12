package com.aliyun.openservices.paifeaturestore.model;

import java.util.Base64;
import java.util.Objects;

import com.aliyun.openservices.paifeaturestore.constants.DatasourceType;
import com.aliyun.tea.utils.StringUtils;
import com.google.gson.annotations.SerializedName;

/**
 * Project
 */

public class  Project {
  @SerializedName("project_id")
  private Long projectId = null;

  @SerializedName("project_name")
  private String projectName = null;

  @SerializedName("workspace_id")
  private String workspaceId = null;

  @SerializedName("description")
  private String description = null;

  @SerializedName("owner")
  private String owner = null;

  @SerializedName("offline_datasource_type")
  private DatasourceType offlineDatasourceType = null;

  @SerializedName("offline_datasource_id")
  private Integer offlineDatasourceId = null;

  @SerializedName("offline_datasource_name")
  private String offlineDatasourceName = null;

  @SerializedName("online_datasource_type")
  private DatasourceType onlineDatasourceType = null;

  @SerializedName("online_datasource_id")
  private Integer onlineDatasourceId = null;

  @SerializedName("online_datasource_name")
  private String onlineDatasourceName = null;

  @SerializedName("offline_lifecycle")
  private Integer offlineLifecycle = null;

  @SerializedName("create_time")
  private String createTime = null;

  @SerializedName("update_time")
  private String updateTime = null;

  @SerializedName("feature_entity_count")
  private Integer featureEntityCount = null;

  @SerializedName("feature_view_count")
  private Integer featureViewCount = null;

  @SerializedName("model_count")
  private Integer modelCount = null;

  @SerializedName("instance_id")
  private String instanceId = null;

  private Datasource offlineDataSource;

  private Datasource onlineDataSource;
  private String signature = null;

  private String FeatureDBAddress = null;
  private String FeatureDBToken =null;
  private String FeatureDBVpcAddress = null;


  public Datasource getOfflineDataSource() {
    return offlineDataSource;
  }

  public void setOfflineDataSource(Datasource offlineDataSource) {
    this.offlineDataSource = offlineDataSource;
  }

  public Datasource getOnlineDataSource() {
    return onlineDataSource;
  }

  public void setOnlineDataSource(Datasource onlineDataSource) {
    this.onlineDataSource = onlineDataSource;
  }

  public Project projectId(Long projectId) {
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

  public Project projectName(String projectName) {
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

  public Project workspaceId(String workspaceId) {
    this.workspaceId = workspaceId;
    return this;
  }

   /**
   * Get workspaceId
   * @return workspaceId
  **/
  public String getWorkspaceId() {
    return workspaceId;
  }

  public void setWorkspaceId(String workspaceId) {
    this.workspaceId = workspaceId;
  }

  public Project description(String description) {
    this.description = description;
    return this;
  }

   /**
   * Get description
   * @return description
  **/
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Project owner(String owner) {
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

  public Project offlineDatasourceType(DatasourceType offlineDatasourceType) {
    this.offlineDatasourceType = offlineDatasourceType;
    return this;
  }

   /**
   * Get offlineDatasourceType
   * @return offlineDatasourceType
  **/
  public DatasourceType getOfflineDatasourceType() {
    return offlineDatasourceType;
  }

  public void setOfflineDatasourceType(DatasourceType offlineDatasourceType) {
    this.offlineDatasourceType = offlineDatasourceType;
  }

  public Project offlineDatasourceId(Integer offlineDatasourceId) {
    this.offlineDatasourceId = offlineDatasourceId;
    return this;
  }

   /**
   * Get offlineDatasourceId
   * @return offlineDatasourceId
  **/
  public Integer getOfflineDatasourceId() {
    return offlineDatasourceId;
  }

  public void setOfflineDatasourceId(Integer offlineDatasourceId) {
    this.offlineDatasourceId = offlineDatasourceId;
  }

  public Project offlineDatasourceName(String offlineDatasourceName) {
    this.offlineDatasourceName = offlineDatasourceName;
    return this;
  }

   /**
   * Get offlineDatasourceName
   * @return offlineDatasourceName
  **/
  public String getOfflineDatasourceName() {
    return offlineDatasourceName;
  }

  public void setOfflineDatasourceName(String offlineDatasourceName) {
    this.offlineDatasourceName = offlineDatasourceName;
  }

  public Project onlineDatasourceType(DatasourceType onlineDatasourceType) {
    this.onlineDatasourceType = onlineDatasourceType;
    return this;
  }

   /**
   * Get onlineDatasourceType
   * @return onlineDatasourceType
  **/
  public DatasourceType getOnlineDatasourceType() {
    return onlineDatasourceType;
  }

  public void setOnlineDatasourceType(DatasourceType onlineDatasourceType) {
    this.onlineDatasourceType = onlineDatasourceType;
  }

  public Project onlineDatasourceId(Integer onlineDatasourceId) {
    this.onlineDatasourceId = onlineDatasourceId;
    return this;
  }

   /**
   * Get onlineDatasourceId
   * @return onlineDatasourceId
  **/
  public Integer getOnlineDatasourceId() {
    return onlineDatasourceId;
  }

  public void setOnlineDatasourceId(Integer onlineDatasourceId) {
    this.onlineDatasourceId = onlineDatasourceId;
  }

  public Project onlineDatasourceName(String onlineDatasourceName) {
    this.onlineDatasourceName = onlineDatasourceName;
    return this;
  }

   /**
   * Get onlineDatasourceName
   * @return onlineDatasourceName
  **/
  public String getOnlineDatasourceName() {
    return onlineDatasourceName;
  }

  public void setOnlineDatasourceName(String onlineDatasourceName) {
    this.onlineDatasourceName = onlineDatasourceName;
  }

  public Project offlineLifecycle(Integer offlineLifecycle) {
    this.offlineLifecycle = offlineLifecycle;
    return this;
  }

   /**
   * Get offlineLifecycle
   * @return offlineLifecycle
  **/
  public Integer getOfflineLifecycle() {
    return offlineLifecycle;
  }

  public void setOfflineLifecycle(Integer offlineLifecycle) {
    this.offlineLifecycle = offlineLifecycle;
  }

  public Project createTime(String createTime) {
    this.createTime = createTime;
    return this;
  }

   /**
   * Get createTime
   * @return createTime
  **/
  public String getCreateTime() {
    return createTime;
  }

  public void setCreateTime(String createTime) {
    this.createTime = createTime;
  }

  public Project updateTime(String updateTime) {
    this.updateTime = updateTime;
    return this;
  }

   /**
   * Get updateTime
   * @return updateTime
  **/
  public String getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
  }

  public Project featureEntityCount(Integer featureEntityCount) {
    this.featureEntityCount = featureEntityCount;
    return this;
  }

   /**
   * Get featureEntityCount
   * @return featureEntityCount
  **/
  public Integer getFeatureEntityCount() {
    return featureEntityCount;
  }

  public void setFeatureEntityCount(Integer featureEntityCount) {
    this.featureEntityCount = featureEntityCount;
  }

  public Project featureViewCount(Integer featureViewCount) {
    this.featureViewCount = featureViewCount;
    return this;
  }

   /**
   * Get featureViewCount
   * @return featureViewCount
  **/
  public Integer getFeatureViewCount() {
    return featureViewCount;
  }

  public void setFeatureViewCount(Integer featureViewCount) {
    this.featureViewCount = featureViewCount;
  }

  public Project modelCount(Integer modelCount) {
    this.modelCount = modelCount;
    return this;
  }

   /**
   * Get modelCount
   * @return modelCount
  **/
  public Integer getModelCount() {
    return modelCount;
  }

  public void setModelCount(Integer modelCount) {
    this.modelCount = modelCount;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Project project = (Project) o;
    return Objects.equals(this.projectId, project.projectId) &&
        Objects.equals(this.projectName, project.projectName) &&
        Objects.equals(this.workspaceId, project.workspaceId) &&
        Objects.equals(this.description, project.description) &&
        Objects.equals(this.owner, project.owner) &&
        Objects.equals(this.offlineDatasourceType, project.offlineDatasourceType) &&
        Objects.equals(this.offlineDatasourceId, project.offlineDatasourceId) &&
        Objects.equals(this.offlineDatasourceName, project.offlineDatasourceName) &&
        Objects.equals(this.onlineDatasourceType, project.onlineDatasourceType) &&
        Objects.equals(this.onlineDatasourceId, project.onlineDatasourceId) &&
        Objects.equals(this.onlineDatasourceName, project.onlineDatasourceName) &&
        Objects.equals(this.offlineLifecycle, project.offlineLifecycle) &&
        Objects.equals(this.createTime, project.createTime) &&
        Objects.equals(this.updateTime, project.updateTime) &&
        Objects.equals(this.featureEntityCount, project.featureEntityCount) &&
        Objects.equals(this.featureViewCount, project.featureViewCount) &&
        Objects.equals(this.instanceId, project.instanceId) &&
        Objects.equals(this.signature, project.signature) &&
        Objects.equals(this.modelCount, project.modelCount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(projectId, projectName, workspaceId, description, owner, offlineDatasourceType, offlineDatasourceId, offlineDatasourceName, onlineDatasourceType, onlineDatasourceId,
            onlineDatasourceName, offlineLifecycle, createTime, updateTime, featureEntityCount, featureViewCount, modelCount, instanceId, signature);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Project {\n");
    
    sb.append("    projectId: ").append(toIndentedString(projectId)).append("\n");
    sb.append("    projectName: ").append(toIndentedString(projectName)).append("\n");
    sb.append("    workspaceId: ").append(toIndentedString(workspaceId)).append("\n");
    sb.append("    description: ").append(toIndentedString(description)).append("\n");
    sb.append("    owner: ").append(toIndentedString(owner)).append("\n");
    sb.append("    offlineDatasourceType: ").append(toIndentedString(offlineDatasourceType)).append("\n");
    sb.append("    offlineDatasourceId: ").append(toIndentedString(offlineDatasourceId)).append("\n");
    sb.append("    offlineDatasourceName: ").append(toIndentedString(offlineDatasourceName)).append("\n");
    sb.append("    onlineDatasourceType: ").append(toIndentedString(onlineDatasourceType)).append("\n");
    sb.append("    onlineDatasourceId: ").append(toIndentedString(onlineDatasourceId)).append("\n");
    sb.append("    onlineDatasourceName: ").append(toIndentedString(onlineDatasourceName)).append("\n");
    sb.append("    offlineLifecycle: ").append(toIndentedString(offlineLifecycle)).append("\n");
    sb.append("    createTime: ").append(toIndentedString(createTime)).append("\n");
    sb.append("    updateTime: ").append(toIndentedString(updateTime)).append("\n");
    sb.append("    featureEntityCount: ").append(toIndentedString(featureEntityCount)).append("\n");
    sb.append("    featureViewCount: ").append(toIndentedString(featureViewCount)).append("\n");
    sb.append("    modelCount: ").append(toIndentedString(modelCount)).append("\n");
    sb.append("    instanceId: ").append(toIndentedString(instanceId)).append("\n");
    sb.append("    signature: ").append(toIndentedString(signature)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  public String getInstanceId() {
        return instanceId;
    }

  public String getSignature() {
    return signature;
  }

  public String getFeatureDBAddress() {
    return FeatureDBAddress;
  }

  public void setFeatureDBAddress(String featureDBAddress) {
    FeatureDBAddress = featureDBAddress;
  }

  public String getFeatureDBToken() {
    return FeatureDBToken;
  }

  public void setFeatureDBToken(String featureDBToken) {
    FeatureDBToken = featureDBToken;
  }

  public String getFeatureDBVpcAddress() {
    return FeatureDBVpcAddress;
  }

  public void setFeatureDBVpcAddress(String featureDBVpcAddress) {
    FeatureDBVpcAddress = featureDBVpcAddress;
  }

  public void createSignature(String username, String password) {
    if (!StringUtils.isEmpty(username) && !(StringUtils.isEmpty(password))) {
      String auth = String.format("%s:%s", username, password);
      this.signature = Base64.getEncoder().encodeToString(auth.getBytes());
    }

  }
}
