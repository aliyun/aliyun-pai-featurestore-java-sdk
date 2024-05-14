package com.aliyun.openservices.paifeaturestore.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FeatureStoreSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                //.useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 2. 定义输入表 'YourSourceTable'
        // 这里为了示例，我们使用 Flink 的 'datagen' 连接器来生成数据
        String createSourceTableSql = ""
                + "CREATE TABLE YourSourceTable ("
                + "  user_id BIGINT,"
                + "  string_field STRING,"
                + "  int32_field INT,"
                + "  float_field FLOAT,"
                + "  double_field DOUBLE,"
                + "  boolean_field BOOLEAN"
                + ") WITH ("
                + "  'connector' = 'datagen',"
                + "  'rows-per-second'='50',"
                + "  'fields.int32_field.kind'='random',"
                + "  'fields.int32_field.min'='1',"
                + "  'fields.int32_field.max'='100'"
                + ")";
        tableEnv.executeSql(createSourceTableSql);

        // 3. 定义输出表 'PrintTable'，使用我们自定义的 'print' 连接器
        String createPrintTableSql = ""
                + "CREATE TABLE PrintTable ("
                + "  user_id BIGINT,"
                + "  string_field STRING,"
                + "  int32_field INT,"
                + "  float_field FLOAT,"
                + "  double_field DOUBLE,"
                + "  boolean_field BOOLEAN"
                + ") WITH ("
                + "  'connector' = 'featurestore',"
                + "  'username' = '${FEATUREDB_USERNAME}',"
                + "  'password' = '${FEATUREDB_PASSWORD}',"
                + "  'region_id' = 'cn-beijing',"
                + "  'aliyun_access_id' = '${ALIBABA_CLOUD_ACCESS_KEY_ID}',"
                + "  'aliyun_access_key' = '${ALIBABA_CLOUD_ACCESS_KEY_SECRET}',"
                + "  'project' = 'fs_demo_featuredb',"
                + "  'host' = 'paifeaturestore.cn-beijing.aliyuncs.com',"
                + "  'use_public_address' = 'true',"
                + "  'feature_view' = 'user_test_1'"
                + ")";
        createPrintTableSql = createPrintTableSql.replace("${FEATUREDB_USERNAME}", Constants.username);
        createPrintTableSql = createPrintTableSql.replace("${FEATUREDB_PASSWORD}", Constants.password);
        createPrintTableSql = createPrintTableSql.replace("${ALIBABA_CLOUD_ACCESS_KEY_ID}", Constants.accessId);
        createPrintTableSql = createPrintTableSql.replace("${ALIBABA_CLOUD_ACCESS_KEY_SECRET}", Constants.accessKey);
        tableEnv.executeSql(createPrintTableSql);

        // 4. 将输入表的数据导入到输出表
        tableEnv.executeSql("INSERT INTO PrintTable SELECT user_id, string_field, int32_field, float_field, double_field,boolean_field FROM YourSourceTable");
        //tableEnv.executeSql("INSERT INTO PrintTable(user_id, string_field, int32_field, boolean_field) SELECT user_id, string_field, int32_field, boolean_field FROM YourSourceTable");

        // 确保应用程序保持运行直到手动停止，否则可能会立即退出
        env.execute("Flink Dynamic Table Sink Application");    }
}
