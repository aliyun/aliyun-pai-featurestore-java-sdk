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

  @SerializedName("parent_id")
  private Integer parentFeatureEntityId = null;

  @SerializedName("parent_name")
  private String parentFeatureEntityName = null;

  @SerializedName("parent_join_id")
  private String parentJoinId = null;


  public FeatureEntity featureEntityId(Integer featureEntityId) {
    this.featureEntityId = featureEntityId;
    return this;
  }

  public Integer getParentFeatureEntityId() {
    return parentFeatureEntityId;
  }

  public void setParentFeatureEntityId(Integer parentFeatureEntityId) {
    this.parentFeatureEntityId = parentFeatureEntityId;
  }

  public String getParentFeatureEntityName() {
    return parentFeatureEntityName;
  }

  public void setParentFeatureEntityName(String parentFeatureEntityName) {
    this.parentFeatureEntityName = parentFeatureEntityName;
  }

  public String getParentJoinId() {
    return parentJoinId;
  }

  public void setParentJoinId(String parentJoinId) {
    this.parentJoinId = parentJoinId;
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
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FeatureEntity that = (FeatureEntity) o;
    return Objects.equals(featureEntityId, that.featureEntityId)
            && Objects.equals(projectId, that.projectId)
            && Objects.equals(projectName, that.projectName)
            && Objects.equals(featureEntityName, that.featureEntityName)
            && Objects.equals(featureEntityJoinid, that.featureEntityJoinid)
            && Objects.equals(owner, that.owner)
            && Objects.equals(createTime, that.createTime)
            && Objects.equals(parentFeatureEntityId, that.parentFeatureEntityId)
            && Objects.equals(parentFeatureEntityName, that.parentFeatureEntityName)
            && Objects.equals(parentJoinId, that.parentJoinId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(featureEntityId, projectId, projectName, featureEntityName, featureEntityJoinid, owner, createTime, parentFeatureEntityId, parentFeatureEntityName, parentJoinId);
  }

  @Override
  public String toString() {
    return "FeatureEntity{" +
            "featureEntityId=" + featureEntityId +
            ", projectId=" + projectId +
            ", projectName='" + projectName + '\'' +
            ", featureEntityName='" + featureEntityName + '\'' +
            ", featureEntityJoinid='" + featureEntityJoinid + '\'' +
            ", owner='" + owner + '\'' +
            ", createTime='" + createTime + '\'' +
            ", parentFeatureEntityId=" + parentFeatureEntityId +
            ", parentFeatureEntityName='" + parentFeatureEntityName + '\'' +
            ", parentJoinId='" + parentJoinId + '\'' +
            '}';
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
