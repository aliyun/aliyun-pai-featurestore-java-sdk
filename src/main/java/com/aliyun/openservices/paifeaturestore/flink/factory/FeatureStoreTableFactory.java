package com.aliyun.openservices.paifeaturestore.flink.factory;

import com.aliyun.openservices.paifeaturestore.flink.sink.FeatureStoreDynamicTableSink;
import com.aliyun.openservices.paifeaturestore.flink.source.FeatureStoreDynamicTableSource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

public class FeatureStoreTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {
    public static final ConfigOption<String> REGIONID = ConfigOptions.key("region_id")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> ACCESSID = ConfigOptions.key("aliyun_access_id")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> ACCESSKEY = ConfigOptions.key("aliyun_access_key")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> HOST = ConfigOptions.key("host")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> PROJECT = ConfigOptions.key("project")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> FEATUREVIEW = ConfigOptions.key("feature_view")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> USERNAME = ConfigOptions.key("username")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<Boolean> USEPUBLICADDRESS = ConfigOptions.key("use_public_address")
            .booleanType()
            .defaultValue(false);
    public static final String IDENTIFIER = "featurestore";
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        final ReadableConfig options = helper.getOptions();
        final String regionId = options.get(REGIONID);
        final String accessId = options.get(ACCESSID);
        final String accessKey = options.get(ACCESSKEY);
        final String project = options.get(PROJECT);
        final String featureView = options.get(FEATUREVIEW);
        final String username = options.get(USERNAME);
        final String password = options.get(PASSWORD);
        boolean usePublicAddress = false;
        if (options.getOptional(USEPUBLICADDRESS).isPresent()) {
            usePublicAddress = options.get(USEPUBLICADDRESS);
        }
        String host = null;
        if (options.getOptional(HOST).isPresent()) {
            host = options.get(HOST);
        }

        final DataType producedDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        return new FeatureStoreDynamicTableSink(regionId, accessId, accessKey, project, featureView, username, password, producedDataType, host, usePublicAddress);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(REGIONID);
        options.add(PROJECT);
        options.add(FEATUREVIEW);
        options.add(ACCESSID);
        options.add(ACCESSKEY);
        options.add(USERNAME);
        options.add(PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOST);
        options.add(USEPUBLICADDRESS);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        final ReadableConfig options = helper.getOptions();
        final String regionId = options.get(REGIONID);
        final String accessId = options.get(ACCESSID);
        final String accessKey = options.get(ACCESSKEY);
        final String project = options.get(PROJECT);
        final String featureView = options.get(FEATUREVIEW);
        final String username = options.get(USERNAME);
        final String password = options.get(PASSWORD);
        boolean usePublicAddress = false;
        if (options.getOptional(USEPUBLICADDRESS).isPresent()) {
            usePublicAddress = options.get(USEPUBLICADDRESS);
        }
        String host = null;
        if (options.getOptional(HOST).isPresent()) {
            host = options.get(HOST);
        }

        TableSchema schema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        return new FeatureStoreDynamicTableSource(regionId, accessId, accessKey, project, featureView, username, password,  host, usePublicAddress,  schema);
    }
}
