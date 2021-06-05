package app.sukuna.sukunaengine.core.segment.generator;

import app.sukuna.sukunaengine.core.index.ImmutableInMemoryIndex;
import app.sukuna.sukunaengine.core.memtable.Memtable;

public interface ISegmentGenerator {
    // TODO: Consider the possibility that a single memtable might be converted to multiple SSTables 
    ImmutableInMemoryIndex fromMemtable(String segmentName, Memtable memtable);
}
