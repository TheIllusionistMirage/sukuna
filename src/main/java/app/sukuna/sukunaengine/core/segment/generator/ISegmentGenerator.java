package app.sukuna.sukunaengine.core.segment.generator;

import app.sukuna.sukunaengine.core.index.ImmutableInMemoryIndex;
import app.sukuna.sukunaengine.core.memtable.Memtable;

public interface ISegmentGenerator {
    ImmutableInMemoryIndex fromMemtable(String segmentName, Memtable memtable);
}
