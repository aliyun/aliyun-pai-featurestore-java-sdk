package com.aliyun.openservices.paifeaturestore.model;

import com.google.gson.annotations.SerializedName;


public class FeatureViewSeqConfig {
    //item_id
    @SerializedName("item_id_field")
    private String ItemIdField;

    //event
    @SerializedName("event_field")
    private String EventField;

    //timestamp
    @SerializedName("timestamp_field")
    private String TimestampField;

    //play_time
    @SerializedName("play_time_field")
    private String PlayTimeField;

    //play_time_filter
    @SerializedName("play_time_filter")
    private String PlayTimeFilter;

    //deduplication_method
    @SerializedName("deduplication_method")
    private String[] DeduplicationMethod;

    //deduplication_method_num
    @SerializedName("deduplication_method_num")
    private int DeduplicationMethodNum;

    //offline_seq_table_name
    @SerializedName("offline_seq_table_name")
    private String OfflineSeqTableName;

    //offline_seq_table_pk_field
    @SerializedName("offline_seq_table_pk_field")
    private String OfflineSeqTablePkField;

    //offline_seq_table_event_time_field
    @SerializedName("offline_seq_table_event_time_field")
    private String OfflineSeqTableEventTimeField;

    //offline_seq_table_partition_field
    @SerializedName("offline_seq_table_partition_field")
    private String OfflineSeqTablePartitionField;

    //seq_len_online
    @SerializedName("seq_len_online")
    private int SeqLenOnline;

    //seq_config
    @SerializedName("seq_config")
    private SeqConfig[] seqConfigs;

    @SerializedName("registration_mode")
    private  String registrationMode;


    @SerializedName("referenced_feature_view_id")
    private  int referencedFeatureViewId;

    @SerializedName("referenced_feature_view_name")
    private  String referencedFeatureViewName;

    public String getItemIdField() {
        return ItemIdField;
    }

    public void setItemIdField(String itemIdField) {
        ItemIdField = itemIdField;
    }

    public String getEventField() {
        return EventField;
    }

    public void setEventField(String eventField) {
        EventField = eventField;
    }

    public String getTimestampField() {
        return TimestampField;
    }

    public void setTimestampField(String timestampField) {
        TimestampField = timestampField;
    }

    public String getPlayTimeField() {
        return PlayTimeField;
    }

    public void setPlayTimeField(String playTimeField) {
        PlayTimeField = playTimeField;
    }

    public String getPlayTimeFilter() {
        return PlayTimeFilter;
    }

    public void setPlayTimeFilter(String playTimeFilter) {
        PlayTimeFilter = playTimeFilter;
    }

    public String[] getDeduplicationMethod() {
        return DeduplicationMethod;
    }

    public void setDeduplicationMethod(String[] deduplicationMethod) {
        DeduplicationMethod = deduplicationMethod;
    }

    public int getDeduplicationMethodNum() {
        return DeduplicationMethodNum;
    }

    public void setDeduplicationMethodNum(int deduplicationMethodNum) {
        DeduplicationMethodNum = deduplicationMethodNum;
    }

    public String getOfflineSeqTableName() {
        return OfflineSeqTableName;
    }

    public void setOfflineSeqTableName(String offlineSeqTableName) {
        OfflineSeqTableName = offlineSeqTableName;
    }

    public String getOfflineSeqTablePkField() {
        return OfflineSeqTablePkField;
    }

    public void setOfflineSeqTablePkField(String offlineSeqTablePkField) {
        OfflineSeqTablePkField = offlineSeqTablePkField;
    }

    public String getOfflineSeqTableEventTimeField() {
        return OfflineSeqTableEventTimeField;
    }

    public void setOfflineSeqTableEventTimeField(String offlineSeqTableEventTimeField) {
        OfflineSeqTableEventTimeField = offlineSeqTableEventTimeField;
    }

    public String getOfflineSeqTablePartitionField() {
        return OfflineSeqTablePartitionField;
    }

    public void setOfflineSeqTablePartitionField(String offlineSeqTablePartitionField) {
        OfflineSeqTablePartitionField = offlineSeqTablePartitionField;
    }

    public int getSeqLenOnline() {
        return SeqLenOnline;
    }

    public void setSeqLenOnline(int seqLenOnline) {
        SeqLenOnline = seqLenOnline;
    }

    public SeqConfig[] getSeqConfigs() {
        return seqConfigs;
    }

    public void setSeqConfigs(SeqConfig[] seqConfigs) {
        this.seqConfigs = seqConfigs;
    }

    public String getRegistrationMode() {
        return registrationMode;
    }

    public void setRegistrationMode(String registrationMode) {
        this.registrationMode = registrationMode;
    }

    public int getReferencedFeatureViewId() {
        return referencedFeatureViewId;
    }

    public void setReferencedFeatureViewId(int referencedFeatureViewId) {
        this.referencedFeatureViewId = referencedFeatureViewId;
    }

    public String getReferencedFeatureViewName() {
        return referencedFeatureViewName;
    }

    public void setReferencedFeatureViewName(String referencedFeatureViewName) {
        this.referencedFeatureViewName = referencedFeatureViewName;
    }
}
