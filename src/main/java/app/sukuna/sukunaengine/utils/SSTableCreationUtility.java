package app.sukuna.sukunaengine.utils;

import java.io.RandomAccessFile;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import app.sukuna.sukunaengine.core.Configuration;

public class SSTableCreationUtility {
    // public IndexBase createIndex() {
    //     //
    // }

    public static void createSegment() {
        String segmentName = "test-sstable.bin";

        TreeMap<String, String> records = new TreeMap<>();
        records.put("apple", "value-apple");
        records.put("ball", "value-ball");
        records.put("cat", "value-cat");
        records.put("dog", "value-dog");
        records.put("elephant", "value-elephant");
        records.put("foot", "value-foot");
        records.put("game", "value-game");
        records.put("hip", "value-hip");
        records.put("illuminati", "value-illuminati");
        records.put("joker", "value-joker");
        records.put("king", "value-king");
        records.put("lemon", "value-lemon");
        records.put("man", "value-man");
        records.put("never", "value-never");
        records.put("octopus", "value-octopus");
        records.put("pot", "value-pot");
        records.put("queen", "value-queen");
        records.put("rust", "value-rust");
        records.put("straw", "value-straw");
        records.put("television", "value-television");
        records.put("uber", "value-uber");
        records.put("violin", "value-violin");
        records.put("water", "value-water");
        records.put("xmas", "value-xmas");
        records.put("yacht", "value-yacht");
        records.put("zero", "value-zero");

        Set<Entry<String, String>> entries = records.entrySet();

        try {
            RandomAccessFile segmentFile = new RandomAccessFile(segmentName, "rw");;

            for (Entry<String,String> entry : entries) {
                String key = entry.getKey();
                String value = entry.getValue();
                byte keyLength = (byte) key.length();
                short valueLength = (short) value.length();
                short recordLength = (short) (keyLength + valueLength + Configuration.SegmentRecordLengthDescriptorSize + Configuration.SegmentKeyLengthDescriptorSize);
                
                segmentFile.writeShort(recordLength);
                segmentFile.writeByte(keyLength);
                segmentFile.write(StringUtils.stringToBinary(key), 0, key.length());
                segmentFile.write(StringUtils.stringToBinary(value), 0, value.length());
            }

            segmentFile.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
