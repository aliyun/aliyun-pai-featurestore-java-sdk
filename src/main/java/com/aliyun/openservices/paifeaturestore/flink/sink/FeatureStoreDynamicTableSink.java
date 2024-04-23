package com.aliyun.openservices.paifeaturestore.flink.sink;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

public class FeatureStoreDynamicTableSink implements DynamicTableSink {
    private String regionId;

    private String accessId;

    private String accessKey;

    private String project;

    private String featureViewName;

    private String username;

    private String password;

    private String host = null;

    private boolean usePublicAddress = false;
    private final DataType dataType;

    public FeatureStoreDynamicTableSink(String regionId, String accessId, String accessKey, String project, String featureView, String username, String password, DataType producedDataType, String host, boolean usePublicAddress)  {
        this.dataType = producedDataType;
        this.regionId = regionId;
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.project = project;
        this.featureViewName = featureView;
        this.username = username;
        this.password = password;
        this.host = host;
        this.usePublicAddress = usePublicAddress;

    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {

        FeatureStoreSinkFunction sinkFunction = new FeatureStoreSinkFunction(this.regionId, this.accessId, this.accessKey, this.project,
                this.featureViewName, this.username, this.password, this.host, this.usePublicAddress, this.dataType);

        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new FeatureStoreDynamicTableSink(this.regionId, this.accessId, this.accessKey, this.project, this.featureViewName, this.username,
                this.password, this.dataType, this.host, this.usePublicAddress);
    }

    @Override
    public String asSummaryString() {
        return FeatureStoreDynamicTableFactory.IDENTIFIER;
    }
}