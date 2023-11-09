/*
 * Feature Store Restful Api
 * PAI-Feature Store is an ML-specific data system server
 *
 */

package com.aliyun.openservices.paifeaturestore.model;

import java.util.Objects;

import com.google.gson.annotations.SerializedName;

/**
 * FeatureEntity
 */

public class FeatureEntity {
  @SerializedName("feature_entity_id")
  private Integer featureEntityId = null;

  @SerializedName("project_id")
  private Long projectId = null;

  @SerializedName("project_name")
  private String projectName = null;

  @SerializedName("feature_entity_name")
  private String featureEntityName = null;

  @SerializedName("feature_entity_joinid")
  private String featureEntityJoinid = null;

  @SerializedName("owner")
  private String owner = null;

  @SerializedName("create_time")
  private String createTime = null;

  public FeatureEntity featureEntityId(Integer featureEntityId) {
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

  public FeatureEntity projectId(Long projectId) {
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

  public FeatureEntity projectName(String projectName) {
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

  public FeatureEntity featureEntityName(String featureEntityName) {
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

  public FeatureEntity featureEntityJoinid(String featureEntityJoinid) {
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

  public FeatureEntity owner(String owner) {
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

  public FeatureEntity createTime(String createTime) {
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


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FeatureEntity featureEntity = (FeatureEntity) o;
    return Objects.equals(this.featureEntityId, featureEntity.featureEntityId) &&
        Objects.equals(this.projectId, featureEntity.projectId) &&
        Objects.equals(this.projectName, featureEntity.projectName) &&
        Objects.equals(this.featureEntityName, featureEntity.featureEntityName) &&
        Objects.equals(this.featureEntityJoinid, featureEntity.featureEntityJoinid) &&
        Objects.equals(this.owner, featureEntity.owner) &&
        Objects.equals(this.createTime, featureEntity.createTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(featureEntityId, projectId, projectName, featureEntityName, featureEntityJoinid, owner, createTime);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class FeatureEntity {\n");
    
    sb.append("    featureEntityId: ").append(toIndentedString(featureEntityId)).append("\n");
    sb.append("    projectId: ").append(toIndentedString(projectId)).append("\n");
    sb.append("    projectName: ").append(toIndentedString(projectName)).append("\n");
    sb.append("    featureEntityName: ").append(toIndentedString(featureEntityName)).append("\n");
    sb.append("    featureEntityJoinid: ").append(toIndentedString(featureEntityJoinid)).append("\n");
    sb.append("    owner: ").append(toIndentedString(owner)).append("\n");
    sb.append("    createTime: ").append(toIndentedString(createTime)).append("\n");
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
