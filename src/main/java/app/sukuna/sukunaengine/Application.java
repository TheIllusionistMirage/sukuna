package app.sukuna.sukunaengine;

import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

	private static final Logger logger = LoggerFactory.getLogger(Application.class);

	public static void main(String[] args) {
		logger.info("Application started running");
		logger.error("test");
		logger.warn("Application finished running");
		LogManager.shutdown();
	}

}
