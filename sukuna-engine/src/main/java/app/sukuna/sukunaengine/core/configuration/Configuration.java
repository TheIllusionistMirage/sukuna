package app.sukuna.sukunaengine.core.configuration;

/**
 * The configuration info for the Sukuna Engine
 */
public class Configuration {
    /**
     * The maximum size of the memtable before it is converted to an SSTable.
     * Default value is 5 MiB.
     */
    public final static long MaxMemtableSizeBeforeSegmentation = 5 * 1024 * 1024;

    /**
     * The block size within a segment. Default value is 4096 B
     */
    public final static int SegmentBlockSize = 4096;

    /**
     * The offset value to use in indexes to indicate records that were present in
     * some segments but were deleted.
     */
    public final static int InvalidSegmentOffset = -1;

    /**
     * The maximum number of segments allowed simultaneously. Default value is 10.
     */
    public final static int MaxSimultaneousSegmentsAllowed = 10;

    /**
     * The time interval between two successive compaction operations. Default value
     * is 30 mins.
     */
    public final static int MinIntervalBetweenConsecutiveCompactionsInMins = 30;

    /**
     * The number of bytes dedicated to describe the length of a record in a
     * segment. Default value is 2.
     */
    public final static int SegmentRecordLengthDescriptorSize = 2;

    /**
     * The number of bytes dedicated to describe the length of a key in a segment.
     * Default value is 1.
     */
    public final static int SegmentKeyLengthDescriptorSize = 1;

    /**
     * The maximum allowed size of a compacted segment. Default value is 10 MiB.
     */
    public final static int MaxCompactedSSTableSizeAllowed = 10 * 1024 * 1024;

    /**
     * The maximum allowed size for the incoming client request queue. Default value
     * is 1024 requests.
     */
    public final static int MaxPendingIncomingRequestQueueSize = 1024;

    /**
     * The maximum allowed size for the read operation queue. Default value is 1024
     * operations.
     */
    public final static int MaxPendingReadOperationQueueSize = 1024;

    /**
     * The maximum allowed size for the write operation queue. Default value is 1024
     * operations.
     */
    public final static int MaxPendingWriteOperationQueueSize = 1024;

    /**
     * The placeholder value in segments to indicate deleted records. Default value
     * is 0xff.
     */
    public final static short DeletedRecordValueInSegmentIndicator = 0xff;
}
