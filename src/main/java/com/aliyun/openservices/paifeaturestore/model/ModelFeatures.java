/*
 * Feature Store Restful Api
 * PAI-Feature Store is an ML-specific data system server
 *
 */

package com.aliyun.openservices.paifeaturestore.model;

import java.util.Objects;

import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.google.gson.annotations.SerializedName;

/**
 * ModelFeatures
 */

public class ModelFeatures {
  @SerializedName("feature_view_id")
  private Integer featureViewId = null;

  @SerializedName("feature_view_name")
  private String featureViewName = null;

  @SerializedName("name")
  private String name = null;

  @SerializedName("alias_name")
  private String aliasName = null;

  @SerializedName("type")
  private FSType type = null;

  @SerializedName("type_str")
  private String typeStr = null;

  public ModelFeatures featureViewId(Integer featureViewId) {
    this.featureViewId = featureViewId;
    return this;
  }

   /**
   * Get featureViewId
   * @return featureViewId
  **/
  public Integer getFeatureViewId() {
    return featureViewId;
  }

  public void setFeatureViewId(Integer featureViewId) {
    this.featureViewId = featureViewId;
  }

  public ModelFeatures featureViewName(String featureViewName) {
    this.featureViewName = featureViewName;
    return this;
  }

   /**
   * Get featureViewName
   * @return featureViewName
  **/
  public String getFeatureViewName() {
    return featureViewName;
  }

  public void setFeatureViewName(String featureViewName) {
    this.featureViewName = featureViewName;
  }

  public ModelFeatures name(String name) {
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

  public ModelFeatures aliasName(String aliasName) {
    this.aliasName = aliasName;
    return this;
  }

   /**
   * Get aliasName
   * @return aliasName
  **/
  public String getAliasName() {
    return aliasName;
  }

  public void setAliasName(String aliasName) {
    this.aliasName = aliasName;
  }

  public ModelFeatures type(FSType type) {
    this.type = type;
    return this;
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

  public ModelFeatures typeStr(String typeStr) {
    this.typeStr = typeStr;
    return this;
  }

   /**
   * Get typeStr
   * @return typeStr
  **/
  public String getTypeStr() {
    return typeStr;
  }

  public void setTypeStr(String typeStr) {
    this.typeStr = typeStr;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ModelFeatures modelFeatures = (ModelFeatures) o;
    return Objects.equals(this.featureViewId, modelFeatures.featureViewId) &&
        Objects.equals(this.featureViewName, modelFeatures.featureViewName) &&
        Objects.equals(this.name, modelFeatures.name) &&
        Objects.equals(this.aliasName, modelFeatures.aliasName) &&
        Objects.equals(this.type, modelFeatures.type) &&
        Objects.equals(this.typeStr, modelFeatures.typeStr);
  }

  @Override
  public int hashCode() {
    return Objects.hash(featureViewId, featureViewName, name, aliasName, type, typeStr);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ModelFeatures {\n");
    
    sb.append("    featureViewId: ").append(toIndentedString(featureViewId)).append("\n");
    sb.append("    featureViewName: ").append(toIndentedString(featureViewName)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    aliasName: ").append(toIndentedString(aliasName)).append("\n");
    sb.append("    type: ").append(toIndentedString(type)).append("\n");
    sb.append("    typeStr: ").append(toIndentedString(typeStr)).append("\n");
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
