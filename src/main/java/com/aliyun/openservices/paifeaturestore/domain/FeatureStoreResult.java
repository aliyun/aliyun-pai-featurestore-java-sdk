package com.aliyun.openservices.paifeaturestore.domain;

import com.aliyun.openservices.paifeaturestore.constants.FSType;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class FeatureStoreResult implements FeatureResult {
    private int currentRow = -1;

    private String[] featureFields;

    private Map<String, FSType> featureFieldTypeMap;

    private List<Map<String, Object>> featureDataList;

    public FSType getType(String featureName) {
        return this.featureFieldTypeMap.get(featureName);
    }

    public boolean isNull(String featureName) {
        return this.featureDataList.get(this.currentRow).get(featureName) == null;
    }

    public Object getObject(String featureName) {
        return this.featureDataList.get(this.currentRow).get(featureName);
    }

    public String getString(String featureName) {
        Object val = this.featureDataList.get(this.currentRow).get(featureName);
        return val == null ? null : val.toString();
    }

    public int getInt(String featureName) {
        Object val = this.featureDataList.get(this.currentRow).get(featureName);
        if (null == val) {
            return 0;
        }
        if (val instanceof Integer) {
            return ((Integer) val).intValue();
        } else if (val instanceof Long) {
            return ((Long) val).intValue();
        } else {
            return Integer.valueOf(val.toString());
        }
    }

    public float getFloat(String featureName) {
        Object val = this.featureDataList.get(this.currentRow).get(featureName);
        if (null == val) {
            return 0.0F;
        }
        if (val instanceof Float) {
            return (float) val;
        } else if (val instanceof Double) {
            return ((Double) val).floatValue();
        } else {
            return Float.valueOf(val.toString());
        }
    }

    public double getDouble(String featureName) {
        Object val = this.featureDataList.get(this.currentRow).get(featureName);
        if (null == val) {
            return 0.0;
        }
        if (val instanceof Double) {
            return (double) val;
        } else if (val instanceof Float) {
            return ((Float) val).doubleValue();
        } else {
            return Double.valueOf(val.toString());
        }
    }

    public long getLong(String featureName) {
        Object val = this.featureDataList.get(this.currentRow).get(featureName);
        if (null == val) {
            return 0L;
        }
        if (val instanceof Long) {
            return (long) val;
        } else if (val instanceof Integer) {
            return ((Integer) val).longValue();
        } else {
            return Long.valueOf(val.toString());
        }
    }

    public boolean getBoolean(String featureName) {
        Object val = this.featureDataList.get(this.currentRow).get(featureName);
        if (null == val) {
            return false;
        }
        if (val instanceof Boolean) {
            return (boolean) val;
        } else {
            return Boolean.valueOf(val.toString());
        }
    }

    public Timestamp getTimestamp(String featureName) {
        Object val = this.featureDataList.get(this.currentRow).get(featureName);
        if (null == val) {
            return null;
        }
        if (val instanceof Timestamp) {
            return (Timestamp) val;
        } else {
            return Timestamp.valueOf(val.toString());
        }
    }

    public boolean next() {
        if (this.currentRow + 1 >= this.featureDataList.size()) {
            return false;
        }
        this.currentRow++;
        return true;
    }

    public void setFeatureFields(String[] featureFields) {
        this.featureFields = featureFields;
    }

    public void setFeatureFieldTypeMap(Map<String, FSType> featureFieldTypeMap) {
        this.featureFieldTypeMap = featureFieldTypeMap;
    }

    public void setFeatureDataList(List<Map<String, Object>> featureDataList) {
        this.featureDataList = featureDataList;
    }

    public List<Map<String, Object>> getFeatureData() {
        return this.featureDataList;
    }

    public String[] getFeatureFields() {
        return featureFields;
    }

    public Map<String, FSType> getFeatureFieldTypeMap() {
        return featureFieldTypeMap;
    }

    public List<Map<String, Object>> getFeatureDataList() {
        return featureDataList;
    }
}
