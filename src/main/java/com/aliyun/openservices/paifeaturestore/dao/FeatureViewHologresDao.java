package com.aliyun.openservices.paifeaturestore.dao;

import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.aliyun.openservices.paifeaturestore.datasource.Hologres;
import com.aliyun.openservices.paifeaturestore.datasource.HologresFactory;
import com.aliyun.openservices.paifeaturestore.domain.FeatureResult;
import com.aliyun.openservices.paifeaturestore.domain.FeatureStoreResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

public class FeatureViewHologresDao implements FeatureViewDao{
    private static Log log = LogFactory.getLog(FeatureViewHologresDao.class);
    private DataSource datasource;
    private String table;
    private String primaryKeyField;
    private String eventTimeField;
    int ttl;

    public Map<String , FSType> fieldTypeMap ;
    public FeatureViewHologresDao(DaoConfig daoConfig) {
        this.table = daoConfig.hologresTableName;
        this.ttl = daoConfig.ttl;
        this.primaryKeyField = daoConfig.primaryKeyField;
        this.eventTimeField = daoConfig.eventTimeField;

        Hologres hologres = HologresFactory.get(daoConfig.hologresName);
        if (null == hologres) {
            throw  new RuntimeException(String.format("hologres:%s not found", daoConfig.hologresName));
        }
        this.datasource = hologres.getDataSource();
        this.fieldTypeMap = daoConfig.fieldTypeMap;
    }

    @Override
    public FeatureResult getFeatures(String[] keys, String[] selectFields) {
        FeatureStoreResult featureResult = new FeatureStoreResult();
        List<Map<String, Object>> featuresList = new ArrayList<>();
        List<Field> fields = new ArrayList<>();
        for (String f : selectFields) {
            fields.add(field(String.format("\"%s\"", f)));
        }

        DSLContext dsl = DSL.using(SQLDialect.POSTGRES);
        Query query = dsl.select(fields)
                .from(table(table))
                .where(field(this.primaryKeyField).in(keys));

        String sql = query.getSQL();
        try(Connection connection = this.datasource.getConnection();
            PreparedStatement statement = connection.prepareStatement(sql)) {
            int pos = 1;
            for (String key : keys) {
                statement.setString(pos++, key);
            }

            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    Map<String, Object> featureMap = new HashMap<>();
                    for (String featureName : selectFields) {
                        if (null == result.getObject(featureName)) {
                            featureMap.put(featureName, null);
                            continue;
                        }
                        switch (this.fieldTypeMap.get(featureName)) {
                            case FS_STRING:
                                featureMap.put(featureName, result.getString(featureName));
                                break;
                            case FS_FLOAT:
                                featureMap.put(featureName, result.getFloat(featureName));
                                break;
                            case FS_INT32:
                                featureMap.put(featureName, result.getInt(featureName));
                                break;
                            case FS_INT64:
                                featureMap.put(featureName, result.getLong(featureName));
                                break;
                            case FS_DOUBLE:
                                featureMap.put(featureName, result.getDouble(featureName));
                                break;
                            case FS_BOOLEAN:
                                featureMap.put(featureName, result.getBoolean(featureName));
                                break;
                            case FS_TIMESTAMP:
                                featureMap.put(featureName, result.getTimestamp(featureName));
                                break;
                        }

                    }

                    featuresList.add(featureMap);
                }
            }
        } catch (Exception e) {
            log.error("getFeatures from hologres error", e);
            throw new RuntimeException(e);
        }
        featureResult.setFeatureFields(selectFields);
        featureResult.setFeatureFieldTypeMap(this.fieldTypeMap);
        featureResult.setFeatureDataList(featuresList);
        return featureResult;
    }
}
