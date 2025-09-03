package com.aliyun.openservices.paifeaturestore.dao;

import com.aliyun.openservices.paifeaturestore.model.FeatureViewSeqConfig;
import com.aliyun.openservices.paifeaturestore.model.SeqConfig;
import com.aliyun.openservices.paifeaturestore.model.SequenceInfo;
import com.aliyun.tea.utils.StringUtils;
import com.sun.org.apache.xpath.internal.operations.Bool;

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


    public Map<String, String> disposeDB(List<SequenceInfo> sequenceInfos, String[] selectFields, FeatureViewSeqConfig config, String event, Long currentime) {
        HashMap<String, String> sequenceFeatures = new HashMap<>();
        HashMap<String, Boolean> sequenceMap = new HashMap<>();
        for (SequenceInfo sequenceInfo : sequenceInfos) {

            String onlineSequenceName = "";
            for (SeqConfig s : config.getSeqConfigs()) {
                if (s.getSeqEvent().equals(event)) {
                    onlineSequenceName = s.getOnlineSeqName();
                    break;
                }
            }

            for (String name : selectFields) {
                String newname = onlineSequenceName + "__" + name;

                if (name.equals(config.getItemIdField())) {
                    if (sequenceFeatures.containsKey(newname)) {
                        sequenceFeatures.put(newname, sequenceFeatures.get(newname) + ";" + sequenceInfo.getItemIdField());
                    } else {
                        sequenceFeatures.put(newname, "" + sequenceInfo.getItemIdField());
                    }
                    if (sequenceFeatures.containsKey(onlineSequenceName)) {
                        sequenceFeatures.put(onlineSequenceName, sequenceFeatures.get(onlineSequenceName) + ";" + sequenceInfo.getItemIdField());
                    } else {
                        sequenceFeatures.put(onlineSequenceName, "" + sequenceInfo.getItemIdField());
                    }
                } else if (name.equals(config.getTimestampField())) {
                    if (sequenceFeatures.containsKey(newname)) {
                        sequenceFeatures.put(newname, sequenceFeatures.get(newname) + ";" + sequenceInfo.getTimestampField());
                    } else {
                        sequenceFeatures.put(newname, "" + sequenceInfo.getTimestampField());
                    }
                } else if (name.equals(config.getEventField())) {
                    if (sequenceFeatures.containsKey(newname)) {
                        sequenceFeatures.put(newname, sequenceFeatures.get(newname) + ";" + sequenceInfo.getEventField());
                    } else {
                        sequenceFeatures.put(newname, sequenceInfo.getEventField());
                    }
                } else if (name.equals(config.getPlayTimeField())) {
                    if (sequenceFeatures.containsKey(newname)) {
                        sequenceFeatures.put(newname, sequenceFeatures.get(newname) + ";" + sequenceInfo.getPlayTimeField());
                    } else {
                        sequenceFeatures.put(newname, "" + sequenceInfo.getPlayTimeField());
                    }

                }
            }
            String tsfields = onlineSequenceName + "__ts";//Timestamp from the current time
            long eventTime = 0;
            if (!StringUtils.isEmpty(sequenceInfo.getTimestampField())) {
                eventTime = Long.valueOf(sequenceInfo.getTimestampField());
            }
            if (sequenceFeatures.containsKey(tsfields)) {
                sequenceFeatures.put(tsfields, sequenceFeatures.get(tsfields) + ";" + (currentime - eventTime));
            } else {
                sequenceFeatures.put(tsfields, String.valueOf((currentime - eventTime)));
            }

            for(String f : sequenceInfo.getOnlineBehaviorTableFields().keySet()){
                if(sequenceFeatures.containsKey(f)){
                    sequenceFeatures.put(f, sequenceFeatures.get(f) + ";" + sequenceInfo.getOnlineBehaviorTableFields().get(f));
                }else{
                    sequenceFeatures.put(f, sequenceInfo.getOnlineBehaviorTableFields().get(f));
                }
            }
        }

        return sequenceFeatures;
    }

}
