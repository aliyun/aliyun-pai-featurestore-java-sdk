package com.aliyun.openservices.paifeaturestore.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FeatureStoreLookupTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                //.useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 2. 定义输入表 'DataGenSource'
        // 这里为了示例，我们使用 Flink 的 'datagen' 连接器来生成数据
        String createSourceTableSql = ""
                + "CREATE TABLE DataGenSource ("
                + "  user_id BIGINT,"
                + "  string_field STRING,"
                + "  int32_field INT,"
                + "  float_field FLOAT,"
                + "  double_field DOUBLE,"
                + "  boolean_field BOOLEAN,"
                + "  proc_time AS PROCTIME(),"
                + " PRIMARY KEY (user_id) NOT ENFORCED"
                + ") WITH ("
                + "  'connector' = 'datagen',"
                + "  'rows-per-second'='1',"
                + "  'fields.int32_field.kind'='random',"
                + "  'fields.int32_field.min'='1',"
                + "  'fields.int32_field.max'='100',"
                + "  'fields.user_id.kind'='random',"
                + "  'fields.user_id.min'='1',"
                + "  'fields.user_id.max'='10'"
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
                + "  boolean_field BOOLEAN,"
                + " PRIMARY KEY (user_id) NOT ENFORCED"
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
        tableEnv.executeSql("INSERT INTO PrintTable SELECT COALESCE(S.user_id, P.user_id), COALESCE(S.string_field, P.string_field), " +
                "COALESCE(S.int32_field, P.int32_field), COALESCE(S.float_field, P.float_field), COALESCE(S.double_field,P.double_field), COALESCE(S.boolean_field, P.boolean_field) " +
                "FROM DataGenSource as S LEFT  JOIN PrintTable  FOR SYSTEM_TIME AS OF  S.proc_time as P ON S.user_id = P.user_id ");

        // 确保应用程序保持运行直到手动停止，否则可能会立即退出
        env.execute("Flink Dynamic Table Sink Application");    }
}
