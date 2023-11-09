package com.aliyun.openservices.paifeaturestore.model;

import java.util.Objects;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;
/**
 * Model
 */

public class Model {
  @SerializedName("model_id")
  private Long modelId = null;

  @SerializedName("project_id")
  private Long projectId = null;

  @SerializedName("project_name")
  private String projectName = null;

  @SerializedName("name")
  private String name = null;

  @SerializedName("owner")
  private String owner = null;

  @SerializedName("label_table_id")
  private Integer labelTableId = null;

  @SerializedName("label_table_name")
  private String labelTableName = null;

  @SerializedName("label_datasource_id")
  private Integer labelDatasourceId = null;

  @SerializedName("label_datasource_table")
  private String labelDatasourceTable = null;

  @SerializedName("label_event_time")
  private String labelEventTime = null;

  @SerializedName("trainning_set_table")
  private String trainningSetTable = null;

  @SerializedName("trainning_set_fg_table")
  private String trainningSetFgTable = null;

  @SerializedName("reserves")
  private String reserves = null;

  @SerializedName("create_time")
  private String createTime = null;

  @SerializedName("update_time")
  private String updateTime = null;

  @SerializedName("features")
  private List<ModelFeatures> features = new ArrayList<ModelFeatures>();

  public Model modelId(Long modelId) {
    this.modelId = modelId;
    return this;
  }

   /**
   * Get modelId
   * @return modelId
  **/
  public Long getModelId() {
    return modelId;
  }

  public void setModelId(Long modelId) {
    this.modelId = modelId;
  }

  public Model projectId(Long projectId) {
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

  public Model projectName(String projectName) {
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

  public Model name(String name) {
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

  public Model owner(String owner) {
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

  public Model labelTableId(Integer labelTableId) {
    this.labelTableId = labelTableId;
    return this;
  }

   /**
   * Get labelTableId
   * @return labelTableId
  **/
  public Integer getLabelTableId() {
    return labelTableId;
  }

  public void setLabelTableId(Integer labelTableId) {
    this.labelTableId = labelTableId;
  }

  public Model labelTableName(String labelTableName) {
    this.labelTableName = labelTableName;
    return this;
  }

   /**
   * Get labelTableName
   * @return labelTableName
  **/
  public String getLabelTableName() {
    return labelTableName;
  }

  public void setLabelTableName(String labelTableName) {
    this.labelTableName = labelTableName;
  }

  public Model labelDatasourceId(Integer labelDatasourceId) {
    this.labelDatasourceId = labelDatasourceId;
    return this;
  }

   /**
   * Get labelDatasourceId
   * @return labelDatasourceId
  **/
  public Integer getLabelDatasourceId() {
    return labelDatasourceId;
  }

  public void setLabelDatasourceId(Integer labelDatasourceId) {
    this.labelDatasourceId = labelDatasourceId;
  }

  public Model labelDatasourceTable(String labelDatasourceTable) {
    this.labelDatasourceTable = labelDatasourceTable;
    return this;
  }

   /**
   * Get labelDatasourceTable
   * @return labelDatasourceTable
  **/
  public String getLabelDatasourceTable() {
    return labelDatasourceTable;
  }

  public void setLabelDatasourceTable(String labelDatasourceTable) {
    this.labelDatasourceTable = labelDatasourceTable;
  }

  public Model labelEventTime(String labelEventTime) {
    this.labelEventTime = labelEventTime;
    return this;
  }

   /**
   * Get labelEventTime
   * @return labelEventTime
  **/
  public String getLabelEventTime() {
    return labelEventTime;
  }

  public void setLabelEventTime(String labelEventTime) {
    this.labelEventTime = labelEventTime;
  }

  public Model trainningSetTable(String trainningSetTable) {
    this.trainningSetTable = trainningSetTable;
    return this;
  }

   /**
   * Get trainningSetTable
   * @return trainningSetTable
  **/
  public String getTrainningSetTable() {
    return trainningSetTable;
  }

  public void setTrainningSetTable(String trainningSetTable) {
    this.trainningSetTable = trainningSetTable;
  }

  public Model trainningSetFgTable(String trainningSetFgTable) {
    this.trainningSetFgTable = trainningSetFgTable;
    return this;
  }

   /**
   * Get trainningSetFgTable
   * @return trainningSetFgTable
  **/
  public String getTrainningSetFgTable() {
    return trainningSetFgTable;
  }

  public void setTrainningSetFgTable(String trainningSetFgTable) {
    this.trainningSetFgTable = trainningSetFgTable;
  }

  public Model reserves(String reserves) {
    this.reserves = reserves;
    return this;
  }

   /**
   * Get reserves
   * @return reserves
  **/
  public String getReserves() {
    return reserves;
  }

  public void setReserves(String reserves) {
    this.reserves = reserves;
  }

  public Model createTime(String createTime) {
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

  public Model updateTime(String updateTime) {
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

  public Model features(List<ModelFeatures> features) {
    this.features = features;
    return this;
  }

  public Model addFeaturesItem(ModelFeatures featuresItem) {
    this.features.add(featuresItem);
    return this;
  }

   /**
   * Get features
   * @return features
  **/
  public List<ModelFeatures> getFeatures() {
    return features;
  }

  public void setFeatures(List<ModelFeatures> features) {
    this.features = features;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Model model = (Model) o;
    return Objects.equals(this.modelId, model.modelId) &&
        Objects.equals(this.projectId, model.projectId) &&
        Objects.equals(this.projectName, model.projectName) &&
        Objects.equals(this.name, model.name) &&
        Objects.equals(this.owner, model.owner) &&
        Objects.equals(this.labelTableId, model.labelTableId) &&
        Objects.equals(this.labelTableName, model.labelTableName) &&
        Objects.equals(this.labelDatasourceId, model.labelDatasourceId) &&
        Objects.equals(this.labelDatasourceTable, model.labelDatasourceTable) &&
        Objects.equals(this.labelEventTime, model.labelEventTime) &&
        Objects.equals(this.trainningSetTable, model.trainningSetTable) &&
        Objects.equals(this.trainningSetFgTable, model.trainningSetFgTable) &&
        Objects.equals(this.reserves, model.reserves) &&
        Objects.equals(this.createTime, model.createTime) &&
        Objects.equals(this.updateTime, model.updateTime) &&
        Objects.equals(this.features, model.features);
  }

  @Override
  public int hashCode() {
    return Objects.hash(modelId, projectId, projectName, name, owner, labelTableId, labelTableName, labelDatasourceId, labelDatasourceTable, labelEventTime, trainningSetTable, trainningSetFgTable, reserves, createTime, updateTime, features);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Model {\n");
    
    sb.append("    modelId: ").append(toIndentedString(modelId)).append("\n");
    sb.append("    projectId: ").append(toIndentedString(projectId)).append("\n");
    sb.append("    projectName: ").append(toIndentedString(projectName)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    owner: ").append(toIndentedString(owner)).append("\n");
    sb.append("    labelTableId: ").append(toIndentedString(labelTableId)).append("\n");
    sb.append("    labelTableName: ").append(toIndentedString(labelTableName)).append("\n");
    sb.append("    labelDatasourceId: ").append(toIndentedString(labelDatasourceId)).append("\n");
    sb.append("    labelDatasourceTable: ").append(toIndentedString(labelDatasourceTable)).append("\n");
    sb.append("    labelEventTime: ").append(toIndentedString(labelEventTime)).append("\n");
    sb.append("    trainningSetTable: ").append(toIndentedString(trainningSetTable)).append("\n");
    sb.append("    trainningSetFgTable: ").append(toIndentedString(trainningSetFgTable)).append("\n");
    sb.append("    reserves: ").append(toIndentedString(reserves)).append("\n");
    sb.append("    createTime: ").append(toIndentedString(createTime)).append("\n");
    sb.append("    updateTime: ").append(toIndentedString(updateTime)).append("\n");
    sb.append("    features: ").append(toIndentedString(features)).append("\n");
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
