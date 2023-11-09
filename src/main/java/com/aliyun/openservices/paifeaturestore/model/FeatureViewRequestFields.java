package com.aliyun.openservices.paifeaturestore.model;

import java.util.Objects;

import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.google.gson.annotations.SerializedName;

/**
 * FeatureViewRequestFields
 */

public class FeatureViewRequestFields {
  @SerializedName("name")
  private String name = null;

  @SerializedName("type")
  private FSType type = null;

  @SerializedName("is_partition")
  private Boolean isPartition = false;

  @SerializedName("is_primary_key")
  private Boolean isPrimaryKey = false;

  @SerializedName("is_event_time")
  private Boolean isEventTime = false;

  private Integer position;

  public FeatureViewRequestFields name(String name) {
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

  public FeatureViewRequestFields type(FSType type) {
    this.type = type;
    return this;
  }

  public Integer getPosition() {
    return position;
  }

  public void setPosition(Integer position) {
    this.position = position;
  }

  /**
   * Get type
   * @return type
  **/
  public FSType getType() {
    return type;
  }

  public void setType(FSType type) {
    this.type = type;
  }

  public FeatureViewRequestFields isPartition(Boolean isPartition) {
    this.isPartition = isPartition;
    return this;
  }

   /**
   * Get isPartition
   * @return isPartition
  **/
  public Boolean isIsPartition() {
    return isPartition;
  }

  public void setIsPartition(Boolean isPartition) {
    this.isPartition = isPartition;
  }

  public FeatureViewRequestFields isPrimaryKey(Boolean isPrimaryKey) {
    this.isPrimaryKey = isPrimaryKey;
    return this;
  }

   /**
   * Get isPrimaryKey
   * @return isPrimaryKey
  **/
  public Boolean isIsPrimaryKey() {
    return isPrimaryKey;
  }

  public void setIsPrimaryKey(Boolean isPrimaryKey) {
    this.isPrimaryKey = isPrimaryKey;
  }

  public FeatureViewRequestFields isEventTime(Boolean isEventTime) {
    this.isEventTime = isEventTime;
    return this;
  }

   /**
   * Get isEventTime
   * @return isEventTime
  **/
  public Boolean isIsEventTime() {
    return isEventTime;
  }

  public void setIsEventTime(Boolean isEventTime) {
    this.isEventTime = isEventTime;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FeatureViewRequestFields featureViewRequestFields = (FeatureViewRequestFields) o;
    return Objects.equals(this.name, featureViewRequestFields.name) &&
        Objects.equals(this.type, featureViewRequestFields.type) &&
        Objects.equals(this.isPartition, featureViewRequestFields.isPartition) &&
        Objects.equals(this.isPrimaryKey, featureViewRequestFields.isPrimaryKey) &&
        Objects.equals(this.isEventTime, featureViewRequestFields.isEventTime) &&
        Objects.equals(this.position, featureViewRequestFields.position);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, isPartition, isPrimaryKey, isEventTime, position);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class FeatureViewRequestFields {\n");
    
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    type: ").append(toIndentedString(type)).append("\n");
    sb.append("    isPartition: ").append(toIndentedString(isPartition)).append("\n");
    sb.append("    isPrimaryKey: ").append(toIndentedString(isPrimaryKey)).append("\n");
    sb.append("    isEventTime: ").append(toIndentedString(isEventTime)).append("\n");
    sb.append("    position: ").append(toIndentedString(position)).append("\n");
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
