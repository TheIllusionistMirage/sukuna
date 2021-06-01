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
		// mtable.upsert("apple", "value-apple");
		// mtable.upsert("ball", "value-ball");
		// mtable.upsert("cat", "value-cat");
		// mtable.upsert("dog", "value-dog");
		// mtable.upsert("elephant", "value-elephant");
		// mtable.upsert("foot", "value-foot");
		// mtable.upsert("game", "value-game");
		// mtable.upsert("hip", "value-hip");
		// mtable.upsert("illuminati", "value-illuminati");
		// mtable.upsert("joker", "value-joker");
		// mtable.upsert("king", "value-king");
		// mtable.upsert("lemon", "value-lemon");
		// mtable.upsert("man", "value-man");
		// mtable.upsert("never", "value-never");
		// mtable.upsert("octopus", "value-octopus");
		// mtable.upsert("pot", "value-pot");
		// mtable.upsert("queen", "value-queen");
		// mtable.upsert("rust", "value-rust");
		// mtable.upsert("straw", "value-straw");
		// mtable.upsert("television", "value-television");
		// mtable.upsert("uber", "value-uber");
		// mtable.upsert("violin", "value-violin");
		// mtable.upsert("water", "value-water");
		// mtable.upsert("xmas", "value-xmas");
		// mtable.upsert("yacht", "value-yacht");
		// mtable.upsert("zero", "value-zero");

		// ISegmentGenerator seggen = new MemtableSegmentGenerator();
		// ImmutableInMemoryIndex index = seggen.fromMemtable("test-seg.bin", mtable);

		ImmutableInMemoryIndex index = new ImmutableInMemoryIndex();
		index.initialize("test-seg.bin");

		// index.printIndex();
		SSTable sstable = new SSTable();
		sstable.initialize("test-seg.bin", index);

		logger.info("apple: " + sstable.read("apple"));
		logger.info("illuminati: " + sstable.read("illuminati"));
		logger.info("queen: " + sstable.read("queen"));

		logger.info("game: " + sstable.read("game"));
		logger.info("king: " + sstable.read("king"));
		logger.info("xmas: " + sstable.read("xmas"));

		sstable.close();

		// SSTableCreationUtility.createSegment();
		// SSTable sstable = new SSTable();
		// InMemoryIndex index = new InMemoryIndex();
		// index.initialize(new String[]{ "apple", "illuminati", "queen" }, new long[]{ 0, 140, 296 });
		// // index.upsertOffset("apple", 0);
		// // index.upsertOffset("illuminati", 140);
		// // index.upsertOffset("queen", 296);
		// sstable.initialize("test-sstable.bin", index);

		// logger.info(sstable.read("apple"));
		// logger.info(sstable.read("illuminati"));
		// logger.info(sstable.read("queen"));

		// logger.info(sstable.read("game"));
		// logger.info(sstable.read("king"));
		// logger.info(sstable.read("xmas"));

		// sstable.close();

		logger.info("Application finished running");
		LogManager.shutdown();
	}
}
