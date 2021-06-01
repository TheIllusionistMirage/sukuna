package app.sukuna.sukunaengine;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.core.index.ImmutableInMemoryIndex;
import app.sukuna.sukunaengine.core.index.InMemoryIndex;
import app.sukuna.sukunaengine.core.memtable.Memtable;
import app.sukuna.sukunaengine.core.segment.SSTable;
import app.sukuna.sukunaengine.core.segment.generator.ISegmentGenerator;
import app.sukuna.sukunaengine.core.segment.generator.MemtableSegmentGenerator;
import app.sukuna.sukunaengine.utils.SSTableCreationUtility;

public class Application {
	private static final Logger logger = LoggerFactory.getLogger(Application.class);

	public static void main(String[] args) throws IOException {
		logger.info("Application started running");
		// logger.error("test");

		// Memtable mtable = new Memtable();
		// mtable.upsert("a", "valA");
		// mtable.upsert("b", "valB");
		// mtable.upsert("c", "valC");
		// mtable.upsert("d", "valD");
		// mtable.upsert("e", "valE");
		// mtable.upsert("f", "valF");
		// mtable.upsert("g", "valG");
		// mtable.upsert("h", "valH");
		// mtable.upsert("i", "valI");
		// mtable.upsert("j", "valJ");

		// ISegmentGenerator seggen = new MemtableSegmentGenerator();
		// seggen.fromMemtable("test-seg.data", mtable);

		SSTableCreationUtility.createSegment();
		SSTable sstable = new SSTable();
		InMemoryIndex index = new InMemoryIndex();
		index.initialize(new String[]{ "apple", "illuminati", "queen" }, new int[]{ 0, 140, 296 });
		// index.upsertOffset("apple", 0);
		// index.upsertOffset("illuminati", 140);
		// index.upsertOffset("queen", 296);
		sstable.initialize("test-sstable.bin", index);

		logger.info(sstable.read("apple"));
		logger.info(sstable.read("illuminati"));
		logger.info(sstable.read("queen"));

		logger.info(sstable.read("game"));
		logger.info(sstable.read("king"));
		logger.info(sstable.read("xmas"));

		sstable.close();

		logger.warn("Application finished running");
		LogManager.shutdown();
	}
}
