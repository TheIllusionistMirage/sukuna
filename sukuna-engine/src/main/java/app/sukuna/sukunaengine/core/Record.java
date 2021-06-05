package app.sukuna.sukunaengine.core;

public class Record {
    public final String containingSegmentName;
    public final int containingSegmentRank;
    public final short recordLength;
    public final byte keyLength;
    public final short valueLength;
    public final byte[] keyByteArray;
    public final byte[] valueByteArray;

    public Record(String containingSegmentName, int containingSegmentRank, short recordLength,
    byte keyLength, short valueLength, byte[] keyByteArray, byte[] valueByteArray) {
        this.containingSegmentName = containingSegmentName;
        this.containingSegmentRank = containingSegmentRank;
        this.recordLength = recordLength;
        this.keyLength = keyLength;
        this.valueLength = valueLength;
        this.keyByteArray = keyByteArray;
        this.valueByteArray = valueByteArray;
    }
}
