package com.aliyun.openservices.paifeaturestore.flink;

import com.aliyun.openservices.paifeaturestore.flink.factory.FeatureStoreTableFactory;
import com.aliyun.openservices.paifeaturestore.flink.sink.FeatureStoreDynamicTableSink;
import com.aliyun.openservices.paifeaturestore.flink.sink.FeatureStoreSinkFunction;
import com.aliyun.openservices.paifeaturestore.flink.source.FeatureStoreAsyncLookupFunction;
import com.aliyun.openservices.paifeaturestore.flink.source.FeatureStoreDynamicTableSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the Flink connector use_private_link option propagation.
 */
public class FeatureStoreTableFactoryPrivateLinkTest {

    @Test
    public void testOptionalOptionsIncludesUsePrivateLinkDefaultFalse() {
        FeatureStoreTableFactory factory = new FeatureStoreTableFactory();
        assertTrue(factory.optionalOptions().contains(FeatureStoreTableFactory.USE_PRIVATE_LINK));
        assertEquals("use_private_link", FeatureStoreTableFactory.USE_PRIVATE_LINK.key());
        assertEquals(Boolean.FALSE, FeatureStoreTableFactory.USE_PRIVATE_LINK.defaultValue());
    }

    @Test
    public void testSourceAndSinkDefaultUsePrivateLinkIsFalse() throws Exception {
        FeatureStoreTableFactory factory = new FeatureStoreTableFactory();
        DynamicTableFactory.Context context = buildContext(Collections.emptyMap());

        FeatureStoreDynamicTableSource source =
                (FeatureStoreDynamicTableSource) factory.createDynamicTableSource(context);
        assertFalse(getBooleanField(source, "usePrivateLink"));
        assertFalse(getBooleanField(source, "usePublicAddress"));
        assertFalse(getBooleanField(source.copy(), "usePrivateLink"));

        FeatureStoreAsyncLookupFunction lookupFunction = extractLookupFunction(source);
        assertFalse(getBooleanField(lookupFunction, "usePrivateLink"));
        assertFalse(getBooleanField(lookupFunction, "usePublicAddress"));

        FeatureStoreDynamicTableSink sink =
                (FeatureStoreDynamicTableSink) factory.createDynamicTableSink(context);
        assertFalse(getBooleanField(sink, "usePrivateLink"));
        assertFalse(getBooleanField(sink, "usePublicAddress"));
        assertFalse(getBooleanField(sink.copy(), "usePrivateLink"));

        SinkFunctionProvider provider = (SinkFunctionProvider) sink.getSinkRuntimeProvider(null);
        FeatureStoreSinkFunction sinkFunction = (FeatureStoreSinkFunction) provider.createSinkFunction();
        assertFalse(getBooleanField(sinkFunction, "usePrivateLink"));
        assertFalse(getBooleanField(sinkFunction, "usePublicAddress"));
    }

    @Test
    public void testSourceAndSinkPropagateUsePrivateLinkTrue() throws Exception {
        FeatureStoreTableFactory factory = new FeatureStoreTableFactory();
        Map<String, String> extra = new HashMap<>();
        extra.put("use_private_link", "true");
        DynamicTableFactory.Context context = buildContext(extra);

        FeatureStoreDynamicTableSource source =
                (FeatureStoreDynamicTableSource) factory.createDynamicTableSource(context);
        assertTrue(getBooleanField(source, "usePrivateLink"));
        assertFalse(getBooleanField(source, "usePublicAddress"));
        DynamicTableSource copiedSource = source.copy();
        assertTrue(getBooleanField(copiedSource, "usePrivateLink"));

        FeatureStoreAsyncLookupFunction lookupFunction =
                extractLookupFunction((FeatureStoreDynamicTableSource) copiedSource);
        assertTrue(getBooleanField(lookupFunction, "usePrivateLink"));
        assertFalse(getBooleanField(lookupFunction, "usePublicAddress"));

        FeatureStoreAsyncLookupFunction deserializedLookup = roundTripSerialize(lookupFunction);
        assertTrue(getBooleanField(deserializedLookup, "usePrivateLink"));
        assertFalse(getBooleanField(deserializedLookup, "usePublicAddress"));

        FeatureStoreDynamicTableSink sink =
                (FeatureStoreDynamicTableSink) factory.createDynamicTableSink(context);
        assertTrue(getBooleanField(sink, "usePrivateLink"));
        assertFalse(getBooleanField(sink, "usePublicAddress"));
        DynamicTableSink copiedSink = sink.copy();
        assertTrue(getBooleanField(copiedSink, "usePrivateLink"));

        SinkFunctionProvider provider = (SinkFunctionProvider) copiedSink.getSinkRuntimeProvider(null);
        FeatureStoreSinkFunction sinkFunction = (FeatureStoreSinkFunction) provider.createSinkFunction();
        assertTrue(getBooleanField(sinkFunction, "usePrivateLink"));
        assertFalse(getBooleanField(sinkFunction, "usePublicAddress"));

        FeatureStoreSinkFunction deserializedSink = roundTripSerialize(sinkFunction);
        assertTrue(getBooleanField(deserializedSink, "usePrivateLink"));
        assertFalse(getBooleanField(deserializedSink, "usePublicAddress"));
    }

    @SuppressWarnings("unchecked")
    private static FeatureStoreAsyncLookupFunction extractLookupFunction(
            FeatureStoreDynamicTableSource source) throws Exception {
        AsyncTableFunctionProvider<?> provider =
                (AsyncTableFunctionProvider<?>) source.getLookupRuntimeProvider(null);
        AsyncTableFunction<?> wrapper = provider.createAsyncTableFunction();
        for (Class<?> c = wrapper.getClass(); c != null; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                f.setAccessible(true);
                Object value = f.get(wrapper);
                if (value instanceof FeatureStoreAsyncLookupFunction) {
                    return (FeatureStoreAsyncLookupFunction) value;
                }
            }
        }
        assertNotNull("FeatureStoreAsyncLookupFunction not found inside wrapper", null);
        return null;
    }

    @SuppressWarnings("unchecked")
    private static <T> T roundTripSerialize(T obj) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
        }
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            return (T) ois.readObject();
        }
    }

    private static DynamicTableFactory.Context buildContext(Map<String, String> extraOptions) {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "featurestore");
        options.put("region_id", "cn-hangzhou");
        options.put("aliyun_access_id", "ak");
        options.put("aliyun_access_key", "sk");
        options.put("project", "test_project");
        options.put("feature_view", "test_fv");
        options.put("username", "user");
        options.put("password", "pass");
        options.putAll(extraOptions);

        ResolvedSchema resolvedSchema = new ResolvedSchema(
                Arrays.asList(
                        Column.physical("user_id", DataTypes.BIGINT().notNull()),
                        Column.physical("age", DataTypes.INT())
                ),
                Collections.emptyList(),
                UniqueConstraint.primaryKey("pk_user_id", Collections.singletonList("user_id"))
        );
        Schema unresolvedSchema = Schema.newBuilder()
                .column("user_id", DataTypes.BIGINT().notNull())
                .column("age", DataTypes.INT())
                .primaryKey("user_id")
                .build();
        CatalogTable origin = CatalogTable.of(unresolvedSchema, "test", Collections.emptyList(), options);
        ResolvedCatalogTable catalogTable = new ResolvedCatalogTable(origin, resolvedSchema);

        return new FactoryUtil.DefaultDynamicTableContext(
                ObjectIdentifier.of("default_catalog", "default_database", "test_table"),
                catalogTable,
                Collections.emptyMap(),
                new Configuration(),
                Thread.currentThread().getContextClassLoader(),
                false
        );
    }

    private static boolean getBooleanField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.getBoolean(target);
    }
}
