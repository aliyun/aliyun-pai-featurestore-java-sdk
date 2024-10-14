package com.aliyun.openservices.paifeaturestore.flink.sink;

import com.aliyun.openservices.paifeaturestore.FeatureStoreClient;
import com.aliyun.openservices.paifeaturestore.api.ApiClient;
import com.aliyun.openservices.paifeaturestore.api.Configuration;
import com.aliyun.openservices.paifeaturestore.domain.FeatureView;
import com.aliyun.openservices.paifeaturestore.domain.Project;
import com.aliyun.openservices.paifeaturestore.domain.SequenceFeatureView;
import com.aliyun.tea.utils.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FeatureStoreSinkFunction implements SinkFunction<RowData> {
    private static final Logger LOG =
            LoggerFactory.getLogger(
                    FeatureStoreSinkFunction.class);
    private String regionId;

    private String accessId;

    private String accessKey;

    private String project;

    private String featureViewName;

    private String username;

    private String password;

    private String host = null;

    private boolean usePublicAddress = false;
    private transient FeatureStoreClient featureStoreClient;

    private transient FeatureView featureView;
    private transient SequenceFeatureView sequenceFeatureView;

    private List<RowType.RowField> fields = new ArrayList<>();
    public FeatureStoreSinkFunction(String regionId, String accessId, String accessKey, String project, String featureViewName, String username, String password, String host, boolean usePublicAddress, DataType dataType) {
        this.regionId = regionId;
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.project = project;
        this.featureViewName = featureViewName;
        this.username = username;
        this.password = password;
        this.host = host;
        this.usePublicAddress = usePublicAddress;
        RowType rowType = (RowType) dataType.getLogicalType();

        // 获取字段名称和类型
        this.fields = rowType.getFields();
    }

    private void initializeFeatureView() {
        if (this.featureStoreClient == null || (this.featureView == null && this.sequenceFeatureView == null)) {
            Configuration configuration = new Configuration(regionId, accessId, accessKey, project);
            if (!StringUtils.isEmpty(host)) {
                configuration.setDomain(host);
            }
            configuration.setPassword(password);
            configuration.setUsername(username);

            ApiClient client = null;
            try {
                client = new ApiClient(configuration);
                this.featureStoreClient = new FeatureStoreClient(client, usePublicAddress);
                Project project1 =  this.featureStoreClient.getProject(project);
                if (null == project1) {
                    throw new RuntimeException(String.format("project:%s, not found", project));
                }
                this.featureView = project1.getFeatureView(featureViewName);
                this.sequenceFeatureView = project1.getSeqFeatureView(featureViewName);

                if (featureView == null && sequenceFeatureView==null){
                    throw new RuntimeException(String.format("featureview:%s, not found", featureViewName));

                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        // lazy init
        initializeFeatureView();

        if (value.getRowKind() == RowKind.INSERT) {
            Map<String, Object> data = new HashMap<>(fields.size());
            for (int i = 0; i < this.fields.size(); i++) {
                RowType.RowField rowField = this.fields.get(i);
                if (rowField.getType() instanceof IntType) {
                    data.put(rowField.getName(), value.getInt(i));
                } else if (rowField.getType() instanceof VarCharType) {
                    data.put(rowField.getName(), value.getString(i).toString());
                } else if (rowField.getType() instanceof DoubleType) {
                    data.put(rowField.getName(), value.getDouble(i));
                } else if (rowField.getType() instanceof FloatType) {
                    data.put(rowField.getName(), value.getFloat(i));
                } else if (rowField.getType() instanceof BigIntType) {
                    data.put(rowField.getName(), value.getLong(i));
                } else if (rowField.getType() instanceof BooleanType) {
                    data.put(rowField.getName(), value.getBoolean(i));
                } else {
                    data.put(rowField.getName(), value.getRawValue(i).toString());
                }
            }
            LOG.debug("write data:{}", data);
            List<Map<String,Object>> content = new ArrayList<>();

            content.add(data);
            if (featureView!=null){
                featureView.writeFeatures(content);
            }
            else {
                sequenceFeatureView.writeFeatures(content);
            }
        }

    }
}
