package app.sukuna.sukunaengine.compactor;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.Configuration;
import app.sukuna.sukunaengine.core.Record;
import app.sukuna.sukunaengine.core.index.ImmutableInMemoryIndex;
import app.sukuna.sukunaengine.core.index.InMemoryIndex;
import app.sukuna.sukunaengine.core.segment.SSTable;
import app.sukuna.sukunaengine.core.segment.SegmentBase;
import app.sukuna.sukunaengine.utils.ErrorHandlingUtils;
import app.sukuna.sukunaengine.utils.IndexUtils;

public class SSTableCompactor implements ICompactor {
    // private List<String> outputSegmentFileNames = new ArrayList<>();
    private static final Logger logger = LoggerFactory.getLogger(SSTableCompactor.class);

    @Override
    public SegmentBase[] compact(SegmentBase[] segments) {
        try {
            List<SSTable> sstables = new ArrayList<>();

            long totalBytesSpanningInputSSTables = 0;
            long totalBytesReadFromInputSSTables = 0;
            long[] segmentFileReadPointers = new long[segments.length];
            // long largestSegmentSizeInInputSegments = 0;
            long[] segmentBytesRead = new long[segments.length];

            for (SegmentBase segment : segments) {
                // if (segment.size > largestSegmentSizeInInputSegments) {
                //     largestSegmentSizeInInputSegments = segment.size;
                // }
                totalBytesSpanningInputSSTables += segment.size;
            }

            // while (true) {
            //     // if (segmentBytesRead)
            // }

            // while (totalBytesReadFromInputSSTables < totalBytesSpanningInputSSTables) { }
            
            // Initialize a new output segment file to write compacted and merged records to
            int currentOutputSegmentIdentifier = 0;

            // outputSegmentFileNames.add("compacted-segment-" + currentOutputSegmentIdentifier++);
            // RandomAccessFile currentOutputSegmentFile = new RandomAccessFile(outputSegmentFileNames.get(currentOutputSegmentIdentifier - 1), "rw");

            RandomAccessFile currentOutputSegmentFile = new RandomAccessFile("compacted-segment-" + currentOutputSegmentIdentifier++, "rw");
            InMemoryIndex currentOutputSegmentIndex = new InMemoryIndex();
            long currentOutputSegmentOffset = 0;

            // The current record being considered for each segment
            TreeMap<String, List<Record>> currentRecords = new TreeMap<>();

            // The current offsets to records read for each segment so far
            HashMap<String, Long> currentSegmentOffsets = new HashMap<>(); 

            HashMap<String, SegmentBase> segmentMap = new HashMap<>();

            for (SegmentBase segment : segments) {
                Record record = this.getNextRecord(segment, 0);
                String recordKey = new String(record.keyByteArray);

                // if (currentRecords.get(recordKey) != null) {
                //     currentRecords.get(recordKey).add(record);
                // }
                // else {
                //     List<Record> records = new ArrayList<>();
                //     records.add(record);
                //     currentRecords.put(recordKey, records);
                // }
                this.addToCurrentRecordsList(currentRecords, record);

                segmentMap.put(segment.name, segment);
                currentSegmentOffsets.put(segment.name, (long) record.recordLength);

                totalBytesReadFromInputSSTables += record.recordLength;
            }

            while (totalBytesReadFromInputSSTables != totalBytesSpanningInputSSTables) {
                // Identify record with lowest key
                Record record = this.getLowestRecord(currentRecords);

                // TODO: Enable this back
                // If current output segment will exceed maximum allowed segment size, then close this segment and create a new one
                if (currentOutputSegmentFile.length() + record.recordLength >= Configuration.MaxCompactedSSTableSizeAllowed) {
                    currentOutputSegmentFile.close();
                    SSTable lastCompactedSSTable = new SSTable();
                    IndexUtils.persistIndexForSegment("compacted-segment-" + (currentOutputSegmentIdentifier - 1), currentOutputSegmentIndex);
                    ImmutableInMemoryIndex index = new ImmutableInMemoryIndex();
                    index.createFrom(currentOutputSegmentIndex);
                    lastCompactedSSTable.initialize("compacted-segment-" + (currentOutputSegmentIdentifier - 1), index);
                    sstables.add(lastCompactedSSTable);
                    currentOutputSegmentOffset = 0;

                    currentOutputSegmentFile = new RandomAccessFile("compacted-segment-" + currentOutputSegmentIdentifier++, "rw");
                }

                // Write the record to the new segment
                this.writeRecord(currentOutputSegmentFile, record, currentOutputSegmentIndex, currentOutputSegmentOffset);
                currentOutputSegmentOffset += record.recordLength;

                // TODO: rethink this
                // Update the count for the total bytes written so far
                // totalBytesReadFromInputSSTables += record.recordLength;

                // Update current records being considered to fetch a new record from relevant segment files
                String lowestKey = currentRecords.firstKey();

                for (Record r : currentRecords.get(lowestKey)) {
                    // Fetch the next record for all segments that this key was present in
                    SegmentBase segment = segmentMap.get(r.containingSegmentName);
                    Record newRecord = this.getNextRecord(segment, currentSegmentOffsets.get(segment.name));
                    
                    this.addToCurrentRecordsList(currentRecords, newRecord);
                    
                    // currentSegmentOffsets.put(segment.name, (long) newRecord.recordLength);
                    this.updateInputSegmentCurrentOffset(currentSegmentOffsets, segment.name, (long) newRecord.recordLength);
                    // totalBytesReadFromInputSSTables += currentSegmentOffsets.get(segment.name);
                    totalBytesReadFromInputSSTables += (long) newRecord.recordLength;
                }

                // Remove record and its versions from the current records being considered
                currentRecords.remove(new String(record.keyByteArray));
            }

            while (currentRecords.size() > 0) {
                // Identify record with lowest key
                Record record = this.getLowestRecord(currentRecords);

                // TODO: Enable this back
                // If current output segment will exceed maximum allowed segment size, then close this segment and create a new one
                if (currentOutputSegmentFile.length() + record.recordLength >= Configuration.MaxCompactedSSTableSizeAllowed) {
                    currentOutputSegmentFile.close();
                    SSTable lastCompactedSSTable = new SSTable();
                    IndexUtils.persistIndexForSegment("compacted-segment-" + (currentOutputSegmentIdentifier - 1), currentOutputSegmentIndex);
                    ImmutableInMemoryIndex index = new ImmutableInMemoryIndex();
                    index.createFrom(currentOutputSegmentIndex);
                    lastCompactedSSTable.initialize("compacted-segment-" + (currentOutputSegmentIdentifier - 1), index);
                    sstables.add(lastCompactedSSTable);
                    currentOutputSegmentOffset = 0;

                    currentOutputSegmentFile = new RandomAccessFile("compacted-segment-" + currentOutputSegmentIdentifier++, "rw");
                }

                // Write the record to the new segment
                this.writeRecord(currentOutputSegmentFile, record, currentOutputSegmentIndex, currentOutputSegmentOffset);
                currentOutputSegmentOffset += record.recordLength;

                // TODO: rethink this
                // Update the count for the total bytes written so far
                // totalBytesReadFromInputSSTables += record.recordLength;

                // Update current records being considered to fetch a new record from relevant segment files
                // String lowestKey = currentRecords.firstKey();

                // for (Record r : currentRecords.get(lowestKey)) {
                //     // Fetch the next record for all segments that this key was present in
                //     SegmentBase segment = segmentMap.get(r.containingSegmentName);
                //     Record newRecord = this.getNextRecord(segment, currentSegmentOffsets.get(segment.name));
                    
                //     this.addToCurrentRecordsList(currentRecords, newRecord);
                    
                //     // currentSegmentOffsets.put(segment.name, (long) newRecord.recordLength);
                //     this.updateInputSegmentCurrentOffset(currentSegmentOffsets, segment.name, (long) newRecord.recordLength);
                //     // totalBytesReadFromInputSSTables += currentSegmentOffsets.get(segment.name);
                //     totalBytesReadFromInputSSTables += (long) newRecord.recordLength;
                // }

                // Remove record and its versions from the current records being considered
                currentRecords.remove(new String(record.keyByteArray));
            }

            // TODO: Iterate over each record left in currentRecords once loop above finishes

            // return (SegmentBase[]) sstables.toArray();
            SSTable[] sstablesArray = new SSTable[sstables.size()];
            for (int i = 0; i < sstables.size(); i++) {
                sstablesArray[i] = sstables.get(i);
            }

            return sstablesArray;
        } catch(Exception exception) {
            // Error
            exception.printStackTrace();
            return null;
        }
    }

    private Record getNextRecord(SegmentBase segment, long currentOffset) {
        try {
            RandomAccessFile segmentFile = segment.getSegmentFile();
            segmentFile.seek(currentOffset);
            
            short recordLength = segmentFile.readShort();
            byte keyLength = segmentFile.readByte();
            short valueLength = (short) (recordLength - keyLength - Configuration.SegmentRecordLengthDescriptorSize - Configuration.SegmentKeyLengthDescriptorSize);

            // Read the key
            byte[] keyByteArray = new byte[keyLength];
            segmentFile.read(keyByteArray, 0, keyLength);

            // Read the value
            byte[] valueByteArray = new byte[valueLength];
            segmentFile.read(valueByteArray, 0, valueLength);
            
            return new Record(segment.name, segment.tableNumber, recordLength, keyLength, valueLength, keyByteArray, valueByteArray);
        } catch(Exception exception) {
            String errorMsg = "Error occurred while reading input file stream: " + segment.name;
            logger.error(ErrorHandlingUtils.getFormattedExceptionDetails(errorMsg, exception));
            return null;
        }
    }

    private void addToCurrentRecordsList(TreeMap<String, List<Record>> currentRecords, Record newRecord) {
        String recordKey = new String(newRecord.keyByteArray);
        if (currentRecords.get(recordKey) != null) {
            currentRecords.get(recordKey).add(newRecord);
        }
        else {
            List<Record> records = new ArrayList<>();
            records.add(newRecord);
            currentRecords.put(recordKey, records);
        }
    }

    private void updateInputSegmentCurrentOffset(HashMap<String, Long> currentSegmentOffsets, String segmentName, long offset) {
        Long currentOffset = currentSegmentOffsets.get(segmentName);

        if (currentOffset == null) {
            currentSegmentOffsets.put(segmentName, offset);
        }
        else {
            currentSegmentOffsets.put(segmentName, currentOffset + offset);
        }
    }

    private Record getLowestRecord(TreeMap<String, List<Record>> records) {
        List<Record> lowestRecord = records.firstEntry().getValue();
        Record latestCopyOfRecord = lowestRecord.get(0);
        // int indexOfLatestCopyOfRecord = 0;
        
        for (int i = 1; i < lowestRecord.size(); i++) {
            if (lowestRecord.get(i).containingSegmentRank > latestCopyOfRecord.containingSegmentRank) {
                latestCopyOfRecord = lowestRecord.get(i);
                // indexOfLatestCopyOfRecord = i;
            }
        }

        return latestCopyOfRecord;
    }

    private void writeRecord(RandomAccessFile currentOutputSegmentFile, Record record, InMemoryIndex index, long currentOutputSegmentOffset) {
        try {
            currentOutputSegmentFile.writeShort(record.recordLength);
            currentOutputSegmentFile.writeByte(record.keyLength);
            currentOutputSegmentFile.write(record.keyByteArray);
            currentOutputSegmentFile.write(record.valueByteArray);

            index.upsertOffset(new String(record.keyByteArray), currentOutputSegmentOffset);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
