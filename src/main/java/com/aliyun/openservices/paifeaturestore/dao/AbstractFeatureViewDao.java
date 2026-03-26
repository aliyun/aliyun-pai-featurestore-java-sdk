package com.aliyun.openservices.paifeaturestore.dao;

import com.aliyun.openservices.paifeaturestore.model.FeatureViewSeqConfig;
import com.aliyun.openservices.paifeaturestore.model.SeqConfig;
import com.aliyun.openservices.paifeaturestore.model.SequenceInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractFeatureViewDao implements FeatureViewDao{
    /*Merge offline and online data. The duplicate part of the timestamp is offline first.
     * @Param offlinesequence(@code list)
     * @Param onlinesequence(@code list)
     * @Param config*/
    public List<SequenceInfo> MergeOnOfflineSeq(List<SequenceInfo> onlineSequence, List<SequenceInfo> offlineSequence,FeatureViewSeqConfig config,String event){

        if (offlineSequence.isEmpty()) {
            return onlineSequence;
        } else if(onlineSequence.isEmpty()) {
            return offlineSequence;
        } else {
            int index=0;
            for (;index<onlineSequence.size();) {
                if (Long.valueOf(onlineSequence.get(index).getTimestampField()) < Long.valueOf(offlineSequence.get(0).getTimestampField())) {
                    break;
                }
                index++;
            }
            onlineSequence=onlineSequence.subList(0,index);
            onlineSequence.addAll(offlineSequence);
            if (onlineSequence.size() > config.getSeqLenOnline()) {
                onlineSequence.subList(0,config.getSeqLenOnline());
            }

        }
        return onlineSequence;
    }


    public Map<String, String> disposeDB(List<SequenceInfo> sequenceInfos, String[] selectFields, FeatureViewSeqConfig config, SeqConfig seqConfig, String event, Long currentime) {
        HashMap<String, StringBuilder> sequenceBuilders = new HashMap<>();

        String onlineSequenceName = "";
        for (SeqConfig s : config.getSeqConfigs()) {
            if (s.getSeqEvent().equals(event)) {
                onlineSequenceName = s.getOnlineSeqName();
                break;
            }
        }

        String tsfields = onlineSequenceName + "__ts";
        HashMap<String, String> fieldNameCache = new HashMap<>();
        for (String name : selectFields) {
            fieldNameCache.put(name, onlineSequenceName + "__" + name);
        }

        for (SequenceInfo sequenceInfo : sequenceInfos) {
            for (String name : selectFields) {
                String newname = fieldNameCache.get(name);

                if (name.equals(config.getItemIdField())) {
                    appendToBuilder(sequenceBuilders, newname, sequenceInfo.getItemIdField());
                    appendToBuilder(sequenceBuilders, onlineSequenceName, sequenceInfo.getItemIdField());
                } else if (name.equals(config.getTimestampField())) {
                    appendToBuilder(sequenceBuilders, newname, String.valueOf(sequenceInfo.getTimestampField()));
                } else if (name.equals(config.getEventField())) {
                    appendToBuilder(sequenceBuilders, newname, sequenceInfo.getEventField());
                } else if (name.equals(config.getPlayTimeField())) {
                    appendToBuilder(sequenceBuilders, newname, String.valueOf(sequenceInfo.getPlayTimeField()));
                }
            }

            // Timestamp from the current time
            long eventTime = 0;
            if (sequenceInfo.getTimestampField() != null) {
                eventTime = sequenceInfo.getTimestampField();
            }
            appendToBuilder(sequenceBuilders, tsfields, String.valueOf(currentime - eventTime));

            if (seqConfig != null && sequenceInfo.getOnlineBehaviorTableFields() != null) {
                for (Map.Entry<String, String> entry : sequenceInfo.getOnlineBehaviorTableFields().entrySet()) {
                    String curSequenceSubName = onlineSequenceName + "__" + entry.getKey();
                    appendToBuilder(sequenceBuilders, curSequenceSubName, entry.getValue());
                }
            }
        }

        HashMap<String, String> sequenceFeatures = new HashMap<>(sequenceBuilders.size());
        for (Map.Entry<String, StringBuilder> entry : sequenceBuilders.entrySet()) {
            sequenceFeatures.put(entry.getKey(), entry.getValue().toString());
        }

        return sequenceFeatures;
    }

    private void appendToBuilder(HashMap<String, StringBuilder> builders, String key, String value) {
        StringBuilder sb = builders.get(key);
        if (sb == null) {
            sb = new StringBuilder(value != null ? value : "");
            builders.put(key, sb);
        } else {
            sb.append(";").append(value != null ? value : "");
        }
    }

}
