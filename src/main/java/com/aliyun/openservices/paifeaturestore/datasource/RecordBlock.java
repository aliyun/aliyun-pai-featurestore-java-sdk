// automatically generated by the FlatBuffers compiler, do not modify

package com.aliyun.openservices.paifeaturestore.datasource;

import com.google.flatbuffers.BaseVector;
import com.google.flatbuffers.BooleanVector;
import com.google.flatbuffers.ByteVector;
import com.google.flatbuffers.Constants;
import com.google.flatbuffers.DoubleVector;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.FloatVector;
import com.google.flatbuffers.IntVector;
import com.google.flatbuffers.LongVector;
import com.google.flatbuffers.ShortVector;
import com.google.flatbuffers.StringVector;
import com.google.flatbuffers.Struct;
import com.google.flatbuffers.Table;
import com.google.flatbuffers.UnionVector;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@SuppressWarnings("unused")
public final class RecordBlock extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_23_5_26(); }
  public static RecordBlock getRootAsRecordBlock(ByteBuffer _bb) { return getRootAsRecordBlock(_bb, new RecordBlock()); }
  public static RecordBlock getRootAsRecordBlock(ByteBuffer _bb, RecordBlock obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public RecordBlock __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public long index() { int o = __offset(4); return o != 0 ? (long)bb.getInt(o + bb_pos) & 0xFFFFFFFFL : 0L; }
  public com.aliyun.openservices.paifeaturestore.datasource.UInt8ValueColumn values(int j) { return values(new com.aliyun.openservices.paifeaturestore.datasource.UInt8ValueColumn(), j); }
  public com.aliyun.openservices.paifeaturestore.datasource.UInt8ValueColumn values(com.aliyun.openservices.paifeaturestore.datasource.UInt8ValueColumn obj, int j) { int o = __offset(6); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int valuesLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public com.aliyun.openservices.paifeaturestore.datasource.UInt8ValueColumn.Vector valuesVector() { return valuesVector(new com.aliyun.openservices.paifeaturestore.datasource.UInt8ValueColumn.Vector()); }
  public com.aliyun.openservices.paifeaturestore.datasource.UInt8ValueColumn.Vector valuesVector(com.aliyun.openservices.paifeaturestore.datasource.UInt8ValueColumn.Vector obj) { int o = __offset(6); return o != 0 ? obj.__assign(__vector(o), 4, bb) : null; }

  public static int createRecordBlock(FlatBufferBuilder builder,
      long index,
      int valuesOffset) {
    builder.startTable(2);
    RecordBlock.addValues(builder, valuesOffset);
    RecordBlock.addIndex(builder, index);
    return RecordBlock.endRecordBlock(builder);
  }

  public static void startRecordBlock(FlatBufferBuilder builder) { builder.startTable(2); }
  public static void addIndex(FlatBufferBuilder builder, long index) { builder.addInt(0, (int) index, (int) 0L); }
  public static void addValues(FlatBufferBuilder builder, int valuesOffset) { builder.addOffset(1, valuesOffset, 0); }
  public static int createValuesVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startValuesVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endRecordBlock(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }
  public static void finishRecordBlockBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
  public static void finishSizePrefixedRecordBlockBuffer(FlatBufferBuilder builder, int offset) { builder.finishSizePrefixed(offset); }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public RecordBlock get(int j) { return get(new RecordBlock(), j); }
    public RecordBlock get(RecordBlock obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}
