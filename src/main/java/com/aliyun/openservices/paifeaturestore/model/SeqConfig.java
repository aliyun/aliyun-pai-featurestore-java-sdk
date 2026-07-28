package com.aliyun.openservices.paifeaturestore.model;

import com.google.gson.annotations.SerializedName;

import java.lang.reflect.Array;
import java.util.ArrayList;

/*  This class contains information about the configuration of sequence features.*/
public class SeqConfig {

    @SerializedName("offline_seq_name")
    private String OfflineSeqName;

    @SerializedName("seq_event")
    private String SeqEvent;

    @SerializedName("seq_len")
    private int SeqLen;

    @SerializedName("online_seq_name")
    private String OnlineSeqName;

    @SerializedName("online_behavior_table_fields")
    private ArrayList<String> OnlineBehaviorTableFields;

    public String getOfflineSeqName() {
        return OfflineSeqName;
    }

    public void setOfflineSeqName(String offlineSeqName) {
        OfflineSeqName = offlineSeqName;
    }

    public String getSeqEvent() {
        return SeqEvent;
    }

    public void setSeqEvent(String seqEvent) {
        SeqEvent = seqEvent;
    }

    public int getSeqLen() {
        return SeqLen;
    }

    public void setSeqLen(int seqLen) {
        SeqLen = seqLen;
    }

    public String getOnlineSeqName() {
        return OnlineSeqName;
    }

    public void setOnlineSeqName(String onlineSeqName) {
        OnlineSeqName = onlineSeqName;
    }

    public ArrayList<String> getOnlineBehaviorTableFields() {
        return OnlineBehaviorTableFields;
    }

    public void setOnlineBehaviorTableFields(ArrayList<String> onlineBehaviorTableFields) {
        OnlineBehaviorTableFields = onlineBehaviorTableFields;
    }
}
