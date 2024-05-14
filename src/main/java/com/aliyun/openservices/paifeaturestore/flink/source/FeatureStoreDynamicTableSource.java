package com.aliyun.openservices.paifeaturestore.flink.source;

import com.alibaba.ververica.connectors.common.dim.AsyncLookupFunctionWrapper;
import com.aliyun.openservices.paifeaturestore.flink.factory.FeatureStoreTableFactory;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;

public class FeatureStoreDynamicTableSource implements LookupTableSource {
    private String regionId;

    private String accessId;

    private String accessKey;

    private String project;

    private String featureViewName;

    private String username;

    private String password;

    private String host = null;

    private boolean usePublicAddress = false;

    private TableSchema tableSchema;

    public FeatureStoreDynamicTableSource(String regionId, String accessId, String accessKey, String project, String featureView, String username, String password, String host, boolean usePublicAddress,  TableSchema schema)  {
        this.regionId = regionId;
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.project = project;
        this.featureViewName = featureView;
        this.username = username;
        this.password = password;
        this.host = host;
        this.usePublicAddress = usePublicAddress;
        this.tableSchema = schema;

    }


    @Override
    public DynamicTableSource copy() {
        return new FeatureStoreDynamicTableSource(this.regionId, this.accessId, this.accessKey, this.project, this.featureViewName, this.username,
                this.password, this.host, this.usePublicAddress, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return FeatureStoreTableFactory.IDENTIFIER;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        FeatureStoreAsyncLookupFunction asyncLookupFunction= new FeatureStoreAsyncLookupFunction(this.regionId, this.accessId, this.accessKey, this.project,
                this.featureViewName, this.username, this.password, this.host, this.usePublicAddress, this.tableSchema);
        AsyncTableFunction<RowData> asyncTableFunction = new  AsyncLookupFunctionWrapper(asyncLookupFunction);
        return AsyncTableFunctionProvider.of(asyncTableFunction);
    }
}
