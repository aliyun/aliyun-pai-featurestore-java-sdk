package com.aliyun.openservices.paifeaturestore.model;

import com.google.gson.annotations.SerializedName;

import java.util.HashMap;

public class SequenceInfo {
    //item_id
    @SerializedName("item_id_field")
    private String ItemIdField;
    //event
    @SerializedName("event_field")
    private String EventField;
    //timestamp
    @SerializedName("timestamp_field")
    private Long TimestampField;
    //play_time
    @SerializedName("play_time_field")
    private Double PlayTimeField;

    @SerializedName("online_behavior_table_fields")
    private HashMap<String,String> OnlineBehaviorTableFields;

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

    public Long getTimestampField() {
        return TimestampField;
    }

    public void setTimestampField(Long timestampField) {
        TimestampField = timestampField;
    }

    public Double getPlayTimeField() {
        return PlayTimeField;
    }

    public void setPlayTimeField(Double playTimeField) {
        PlayTimeField = playTimeField;
    }

    public HashMap<String, String> getOnlineBehaviorTableFields() {
        return OnlineBehaviorTableFields;
    }

    public void setOnlineBehaviorTableFields(HashMap<String, String> onlineBehaviorTableFields) {
        OnlineBehaviorTableFields = onlineBehaviorTableFields;
    }
}
