package app.sukuna.sukunaengine.compactor;

import app.sukuna.sukunaengine.core.segment.SegmentBase;

public interface ICompactor {
    SegmentBase[] compact(SegmentBase[] segments);
}
