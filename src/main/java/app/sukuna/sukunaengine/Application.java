package app.sukuna.sukunaengine;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.sukuna.sukunaengine.compactor.SSTableCompactor;
import app.sukuna.sukunaengine.core.index.ImmutableInMemoryIndex;
import app.sukuna.sukunaengine.core.index.InMemoryIndex;
import app.sukuna.sukunaengine.core.memtable.Memtable;
import app.sukuna.sukunaengine.core.segment.SSTable;
import app.sukuna.sukunaengine.core.segment.SegmentBase;
import app.sukuna.sukunaengine.core.segment.generator.ISegmentGenerator;
import app.sukuna.sukunaengine.core.segment.generator.MemtableSegmentGenerator;
import app.sukuna.sukunaengine.service.SukunaService;
import app.sukuna.sukunaengine.utils.SSTableCreationUtility;

public class Application {
	private static final Logger logger = LoggerFactory.getLogger(Application.class);

	public static void main(String[] args) throws IOException {
		logger.info("Application started running");
		// logger.error("test");

		// Memtable mtable = new Memtable();
		// mtable.upsert("appleb", "value-B-UPDATED-apple");
		// mtable.upsert("ballb", "value-B-UPDATED-ball");
		// mtable.upsert("catb", "value-B-UPDATED-cat");
		// mtable.upsert("dogb", "value-B-UPDATED-dog");
		// mtable.upsert("elephantb", "value-B-UPDATED-elephant");
		// mtable.upsert("footb", "value-B-UPDATED-foot");
		// mtable.upsert("gameb", "value-B-UPDATED-game");
		// mtable.upsert("hipb", "value-B-UPDATED-hip");
		// mtable.upsert("illuminatib", "value-B-UPDATED-illuminati");
		// mtable.upsert("jokerb", "value-B-UPDATED-joker");
		// mtable.upsert("kingb", "value-B-UPDATED-king");
		// mtable.upsert("lemonb", "value-B-UPDATED-lemon");
		// mtable.upsert("manb", "value-B-UPDATED-man");
		// mtable.upsert("neverb", "value-B-UPDATED-never");
		// mtable.upsert("octopusb", "value-B-UPDATED-octopus");
		// mtable.upsert("potb", "value-B-UPDATED-pot");
		// mtable.upsert("queenb", "value-B-UPDATED-queen");
		// mtable.upsert("rustb", "value-B-UPDATED-rust");
		// mtable.upsert("strawb", "value-B-UPDATED-straw");
		// mtable.upsert("televisionb", "value-B-UPDATED-television");
		// mtable.upsert("uberb", "value-B-UPDATED-uber");
		// mtable.upsert("violinb", "value-B-UPDATED-violin");
		// mtable.upsert("waterb", "value-B-UPDATED-water");
		// mtable.upsert("xmasb", "value-B-UPDATED-xmas");
		// mtable.upsert("yachtb", "value-B-UPDATED-yacht");
		// mtable.upsert("zerob", "value-B-UPDATED-zero");

		// ISegmentGenerator seggen = new MemtableSegmentGenerator();
		// ImmutableInMemoryIndex index = seggen.fromMemtable("test4-seg.bin", mtable);

		// ImmutableInMemoryIndex index = new ImmutableInMemoryIndex();
		// index.initialize("test-seg.bin");

		// // index.printIndex();
		// SSTable sstable = new SSTable();
		// sstable.initialize("test-seg.bin", index);

		// logger.info("apple: " + sstable.read("apple"));
		// logger.info("illuminati: " + sstable.read("illuminati"));
		// logger.info("queen: " + sstable.read("queen"));

		// logger.info("game: " + sstable.read("game"));
		// logger.info("king: " + sstable.read("king"));
		// logger.info("xmas: " + sstable.read("xmas"));

		// sstable.close();

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

		/////

		// ImmutableInMemoryIndex index2 = new ImmutableInMemoryIndex();
		// index2.initialize("test2-seg.bin");

		// ImmutableInMemoryIndex index3 = new ImmutableInMemoryIndex();
		// index3.initialize("test3-seg.bin");

		// ImmutableInMemoryIndex index4 = new ImmutableInMemoryIndex();
		// index4.initialize("test4-seg.bin");

		// SSTable sstable2 = new SSTable();
		// sstable2.initialize("test2-seg.bin", index2);
		// sstable2.tableNumber = 2;

		// SSTable sstable3 = new SSTable();
		// sstable3.initialize("test3-seg.bin", index3);
		// sstable3.tableNumber = 3;

		// SSTable sstable4 = new SSTable();
		// sstable4.initialize("test4-seg.bin", index4);
		// sstable4.tableNumber = 4;

		// SSTableCompactor compactor = new SSTableCompactor();
		// SegmentBase[] compactedSSTables = compactor.compact(new SegmentBase[]{ sstable2, sstable3, sstable4 });
		// for (SegmentBase compactedSegment : compactedSSTables) {
		// 	compactedSegment.close();
		// }

		SukunaService service = new SukunaService();
		service.start();

		logger.info("Application finished running");
		LogManager.shutdown();
	}
}
