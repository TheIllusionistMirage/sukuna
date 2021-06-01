package app.sukuna.sukunaengine.core.segment.generator;

import app.sukuna.sukunaengine.core.memtable.Memtable;

public interface ISegmentGenerator {
    void fromMemtable(String segmentName, Memtable memtable);
}
