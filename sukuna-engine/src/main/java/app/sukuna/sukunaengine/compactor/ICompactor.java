package app.sukuna.sukunaengine.compactor;

import app.sukuna.sukunaengine.core.segment.SegmentBase;

public interface ICompactor {
    // TODO: Switch to IList
    SegmentBase[] compact(SegmentBase[] segments);
}
