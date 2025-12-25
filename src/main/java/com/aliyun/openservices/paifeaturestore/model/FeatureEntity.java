/*
 * Feature Store Restful Api
 * PAI-Feature Store is an ML-specific data system server
 *
 */

package com.aliyun.openservices.paifeaturestore.model;

import java.util.ArrayList;
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
  private Integer ParentFeatureEntityId = null;

  @SerializedName("parent_name")
  private String ParentFeatureEntityName = null;

  @SerializedName("parent_join_id")
  private String ParentJoinId = null;


  public FeatureEntity featureEntityId(Integer featureEntityId) {
    this.featureEntityId = featureEntityId;
    return this;
  }

  public Integer getParentFeatureEntityId() {
    return ParentFeatureEntityId;
  }

  public void setParentFeatureEntityId(Integer parentFeatureEntityId) {
    ParentFeatureEntityId = parentFeatureEntityId;
  }

  public String getParentFeatureEntityName() {
    return ParentFeatureEntityName;
  }

  public void setParentFeatureEntityName(String parentFeatureEntityName) {
    ParentFeatureEntityName = parentFeatureEntityName;
  }

  public String getParentJoinId() {
    return ParentJoinId;
  }

  public void setParentJoinId(String parentJoinId) {
    ParentJoinId = parentJoinId;
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
            && Objects.equals(ParentFeatureEntityId, that.ParentFeatureEntityId)
            && Objects.equals(ParentFeatureEntityName, that.ParentFeatureEntityName)
            && Objects.equals(ParentJoinId, that.ParentJoinId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(featureEntityId, projectId, projectName, featureEntityName, featureEntityJoinid, owner, createTime, ParentFeatureEntityId, ParentFeatureEntityName, ParentJoinId);
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
            ", ParentId=" + ParentFeatureEntityId +
            ", ParentName='" + ParentFeatureEntityName + '\'' +
            ", ParentJoinId='" + ParentJoinId + '\'' +
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
