package app.sukuna.sukunaengine.core.segment;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.index.IndexBase;

public abstract class SegmentBase {
    protected String name;
    protected IndexBase index;
    // TODO: Use FileChannel from NIO to support concurrent file access 
    protected RandomAccessFile segmentFile;
    private static final Logger logger = LoggerFactory.getLogger(SegmentBase.class);
    
    public abstract void initialize(String segmentName, IndexBase index);
    public abstract String read(String key);
    public abstract void close() throws IOException;

    @Override
    public void finalize() throws IOException {
        this.segmentFile.close();
        logger.info("Closed input stream for segment file: " + this.name);
    }
}
