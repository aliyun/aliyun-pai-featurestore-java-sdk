package com.aliyun.openservices.paifeaturestore.model;

import java.util.Objects;

import com.alicloud.openservices.tablestore.SyncClient;
import com.aliyun.igraph.client.gremlin.driver.Client;
import com.aliyun.igraph.client.gremlin.driver.Cluster;
import com.aliyun.openservices.paifeaturestore.constants.DatasourceType;
import com.aliyun.openservices.paifeaturestore.datasource.FeatureDBClient;
import com.aliyun.openservices.paifeaturestore.datasource.HttpConfig;
import com.google.gson.annotations.SerializedName;

/**
 * Datasource
 */


public class Datasource {
  @SerializedName("datasource_id")
  private Integer datasourceId = null;

  @SerializedName("type")
  private DatasourceType type = null;

  @SerializedName("name")
  private String name = null;

  @SerializedName("workspace_id")
  private String workspaceId = null;

  @SerializedName("region")
  private String region = null;

  @SerializedName("vpc_address")
  private String vpcAddress = null;

  @SerializedName("public_address")
  private String publicAddress = null;

  @SerializedName("project")
  private String project = null;

  @SerializedName("database")
  private String database = null;

  @SerializedName("token")
  private String token = null;

  @SerializedName("user")
  private String user = null;

  @SerializedName("pwd")
  private String pwd = null;

  @SerializedName("rds_instance_id")
  private String rdsInstanceId = null;

  @SerializedName("endpoint")
  private String endpoint = null;

  @SerializedName("fdb_vpc_plk_address")
  private String fdbVpcAddress;

  public String getFdbVpcAddress() {
    return fdbVpcAddress;
  }

  public void setFdbVpcAddress(String fdbVpcAddress) {
    this.fdbVpcAddress = fdbVpcAddress;
  }

  private AK ak;

  public AK getAk() {
    return ak;
  }

  public void setAk(AK ak) {
    this.ak = ak;
  }

  public Datasource datasourceId(Integer datasourceId) {
    this.datasourceId = datasourceId;
    return this;
  }

   /**
   * Get datasourceId
   * @return datasourceId
  **/
  public Integer getDatasourceId() {
    return datasourceId;
  }

  public void setDatasourceId(Integer datasourceId) {
    this.datasourceId = datasourceId;
  }

  public Datasource type(DatasourceType type) {
    this.type = type;
    return this;
  }

   /**
   * Get type
   * @return type
  **/
  public DatasourceType getType() {
    return type;
  }

  public void setType(DatasourceType type) {
    this.type = type;
  }

  public Datasource name(String name) {
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

  public Datasource workspaceId(String workspaceId) {
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

  public Datasource region(String region) {
    this.region = region;
    return this;
  }

   /**
   * Get region
   * @return region
  **/
  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public Datasource vpcAddress(String vpcAddress) {
    this.vpcAddress = vpcAddress;
    return this;
  }

   /**
   * Get vpcAddress
   * @return vpcAddress
  **/
  public String getVpcAddress() {
    return vpcAddress;
  }

  public void setVpcAddress(String vpcAddress) {
    this.vpcAddress = vpcAddress;
  }

  public Datasource publicAddress(String publicAddress) {
    this.publicAddress = publicAddress;
    return this;
  }

   /**
   * Get publicAddress
   * @return publicAddress
  **/
  public String getPublicAddress() {
    return publicAddress;
  }

  public void setPublicAddress(String publicAddress) {
    this.publicAddress = publicAddress;
  }


  public Datasource project(String project) {
    this.project = project;
    return this;
  }

   /**
   * Get project
   * @return project
  **/
  public String getProject() {
    return project;
  }

  public void setProject(String project) {
    this.project = project;
  }

  public Datasource database(String database) {
    this.database = database;
    return this;
  }

   /**
   * Get database
   * @return database
  **/
  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public Datasource token(String token) {
    this.token = token;
    return this;
  }

   /**
   * Get token
   * @return token
  **/
  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  public Datasource user(String user) {
    this.user = user;
    return this;
  }

   /**
   * Get user
   * @return user
  **/
  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public Datasource pwd(String pwd) {
    this.pwd = pwd;
    return this;
  }

   /**
   * Get pwd
   * @return pwd
  **/
  public String getPwd() {
    return pwd;
  }

  public void setPwd(String pwd) {
    this.pwd = pwd;
  }

  public Datasource rdsInstanceId(String rdsInstanceId) {
    this.rdsInstanceId = rdsInstanceId;
    return this;
  }

   /**
   * Get rdsInstanceId
   * @return rdsInstanceId
  **/
  public String getRdsInstanceId() {
    return rdsInstanceId;
  }

  public void setRdsInstanceId(String rdsInstanceId) {
    this.rdsInstanceId = rdsInstanceId;
  }

  public Datasource endpoint(String endpoint) {
    this.endpoint = endpoint;
    return this;
  }

   /**
   * Get endpoint
   * @return endpoint
  **/
  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Datasource datasource = (Datasource) o;
    return Objects.equals(this.datasourceId, datasource.datasourceId) &&
        Objects.equals(this.type, datasource.type) &&
        Objects.equals(this.name, datasource.name) &&
        Objects.equals(this.workspaceId, datasource.workspaceId) &&
        Objects.equals(this.region, datasource.region) &&
        Objects.equals(this.vpcAddress, datasource.vpcAddress) &&
        Objects.equals(this.publicAddress, datasource.publicAddress) &&
        Objects.equals(this.project, datasource.project) &&
        Objects.equals(this.database, datasource.database) &&
        Objects.equals(this.token, datasource.token) &&
        Objects.equals(this.user, datasource.user) &&
        Objects.equals(this.pwd, datasource.pwd) &&
        Objects.equals(this.rdsInstanceId, datasource.rdsInstanceId) &&
        Objects.equals(this.endpoint, datasource.endpoint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(datasourceId, type, name, workspaceId, region, vpcAddress, publicAddress, project, database, token,  user, pwd, rdsInstanceId, endpoint);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Datasource {\n");
    
    sb.append("    datasourceId: ").append(toIndentedString(datasourceId)).append("\n");
    sb.append("    type: ").append(toIndentedString(type)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    workspaceId: ").append(toIndentedString(workspaceId)).append("\n");
    sb.append("    region: ").append(toIndentedString(region)).append("\n");
    sb.append("    vpcAddress: ").append(toIndentedString(vpcAddress)).append("\n");
    sb.append("    publicAddress: ").append(toIndentedString(publicAddress)).append("\n");
    sb.append("    project: ").append(toIndentedString(project)).append("\n");
    sb.append("    database: ").append(toIndentedString(database)).append("\n");
    sb.append("    token: ").append(toIndentedString(token)).append("\n");
    sb.append("    user: ").append(toIndentedString(user)).append("\n");
    sb.append("    pwd: ").append(toIndentedString(pwd)).append("\n");
    sb.append("    rdsInstanceId: ").append(toIndentedString(rdsInstanceId)).append("\n");
    sb.append("    endpoint: ").append(toIndentedString(endpoint)).append("\n");
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

  public String  generateDSN(DatasourceType type,boolean usePublicAddress)  {
      if (usePublicAddress) {
        this.vpcAddress=this.publicAddress;
      }
      String dsn = null;
      switch (type) {
        case Datasource_Type_Hologres:
          dsn = String.format("jdbc:postgresql://%s/%s?user=%s&password=%s&ApplicationName=PAI-FeatureStore",
                  this.vpcAddress, this.database, this.ak.getAccessId(), this.ak.getAccessKey());
          break;
      }

     return dsn;
  }

  public SyncClient generateOTSClient(boolean usePublicAddress) {
      if (usePublicAddress) {
        this.vpcAddress=this.publicAddress;
      }
      return new SyncClient(this.vpcAddress, this.ak.getAccessId(), this.ak.getAccessKey(), this.rdsInstanceId );
  }

  public Client generateIgraphClient(boolean usePublicAddress) {
    if (usePublicAddress) {
      this.vpcAddress=this.publicAddress;
    }
    Cluster.Builder builder = Cluster.build();
    String endpoint = this.vpcAddress;
    builder.addContactPoint(endpoint).userName(this.user).userPasswd(this.pwd)
            .retryTimes(3).connectionRequestTimeout(2000).maxConnTotal(10000).maxConnPerRoute(5000);
    builder.src("PAI-FeatureStore-Java");
    Cluster cluster = builder.create();
    Client client = cluster.connect();
    return client;
  }

  public FeatureDBClient generateFeatureDBClient(boolean usePublicAddress) {

    FeatureDBClient featureDBClient = new FeatureDBClient(new HttpConfig());

    if (usePublicAddress) {
      featureDBClient.setAddress(this.publicAddress);
    } else {
      if (null == this.fdbVpcAddress) {
          featureDBClient.setAddress(this.vpcAddress);
      } else {
        // check
        featureDBClient.setVpcAddress(String.format("http://%s",this.vpcAddress));
        Boolean isConnected = featureDBClient.CheckVpcAddress();
        if (!isConnected) {
          featureDBClient.setAddress(this.vpcAddress);
        }
      }
    }
    featureDBClient.setToken(this.token);
    return featureDBClient;
  }
}
