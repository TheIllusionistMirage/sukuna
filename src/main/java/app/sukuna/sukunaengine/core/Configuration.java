package app.sukuna.sukunaengine.core;

public class Configuration {
    public static int MaxLogSizeBeforeCompaction;
    public static int MaxMemtableSizeBeforeSegmentation = 1 * 1024 * 1024; // 5MB is the max allowed memtable size before it is written to a SSTable 
    public static int SegmentBlockSize = 120;
    public static int CompactionInterval;
    public static int MergingInterval;
    public static int InvalidIndexOffset = -1;
    // TODO: Move this to SegmentBase/SSTable
    public final static int SegmentRecordLengthDescriptorSize = 2; // in bytes
    public final static int SegmentKeyLengthDescriptorSize = 1;  // in bytes
    public final static int MaxSegmentsAllowed = 4;
    public final static int MaxCompactedSSTableSizeAllowed = 1024;
    public final static int PendingReadOperationQueueSize = 1024;
    public final static int PendingWriteOperationQueueSize = 1024;
    public final static long InvalidSegmentOffsetIndicator = -1;
    public final static long DeletedIndexedRecordOffsetIndicator = -1;
    public final static short DeletedRecordValueInSegmentIndicator = 0xff;
    public final static int SukunaServicePort = 6969;
}
