package app.sukuna.sukunaengine.core.segment;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.index.IndexBase;

public abstract class SegmentBase {
    // TODO: Make this field final
    public String name;
    protected IndexBase index;
    // TODO: Make this field final
    public int rank; // Rank of SST, higher the rank the more recent it is
    public long size; // segment size on disk
    // TODO: Use FileChannel from NIO to support concurrent file access 
    protected RandomAccessFile segmentFile;
    private static final Logger logger = LoggerFactory.getLogger(SegmentBase.class);

    // public SegmentBase(String name, int rank) {
    //     this.name = name;
    //     this.rank = rank;
    // }

    public abstract void initialize(String segmentName, IndexBase index);
    public abstract String read(String key);
    public abstract void close() throws IOException;
    
    // Hack for now
    public RandomAccessFile getSegmentFile() {
        return this.segmentFile;
    }

    @Override
    public void finalize() throws IOException {
        this.segmentFile.close();
        logger.info("Closed input stream for segment file: " + this.name);
    }
}
