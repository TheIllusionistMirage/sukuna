package app.sukuna.sukunaengine.core;

public class Configuration {
    public static int MaxLogSizeBeforeCompaction;
    public static int MaxMemtableSizeBeforeSegmentation = 100 * 1024; // 100KB is the max allowed memtable size before it is written to a SSTable 
    public static int SegmentBlockSize = 4096; // in bytes
    public static int CompactionInterval;
    public static int MergingInterval;
    public static int InvalidIndexOffset = -1;
    // TODO: Move this to SegmentBase/SSTable
    public static int MaxSimultaneousSSTablesAllowed = 10;
    public static int MinIntervalBetweenConsecutiveCompactions = 30; // in minutes
    public static int SegmentRecordLengthDescriptorSize = 2; // in bytes
    public static int SegmentKeyLengthDescriptorSize = 1;  // in bytes
    // public static int MaxSegmentsAllowed = 4;
    public static int MaxCompactedSSTableSizeAllowed = 4096 * 100;
    public static int MaxPendingReadOperationQueueSize = 1024;
    public static int MaxPendingWriteOperationQueueSize = 1024;
    public static long InvalidSegmentOffsetIndicator = -1;
    public static long DeletedIndexedRecordOffsetIndicator = -1;
    public static short DeletedRecordValueInSegmentIndicator = 0xff;
    public static int SukunaServicePort = 6969;
}
