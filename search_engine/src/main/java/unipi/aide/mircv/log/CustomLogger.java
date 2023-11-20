package unipi.aide.mircv.log;

import unipi.aide.mircv.helpers.StreamHelper;

import java.io.IOException;
import java.util.logging.*;

public class CustomLogger{
    private static Logger logger;

    static {
        // logger configuration
        logger = Logger.getLogger(Logger.class.getName());
        logger.setLevel(Level.ALL);

        // Creation of console logger handler
        ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.ALL);
        consoleHandler.setFormatter(new SimpleFormatter() {
            private final String format = "[%1$tF %1$tT] -- %2$-7s -- %3$s %n";

            @Override
            public synchronized String format(LogRecord lr) {
                return String.format(format,
                        new java.util.Date(lr.getMillis()),
                        lr.getLevel().getName(), // Get log level
                        lr.getMessage()
                );
            }
        });
        logger.addHandler(consoleHandler);
    }

    /***
     *
     * @param logFilePath directory where put the logger file
     */
    public static void configureFileLogger(String logFilePath) {
        if (logFilePath != null) {
            try {
                StreamHelper.createDir(logFilePath);
                // Creation of log handler for the specify dir
                FileHandler fileHandler = new FileHandler(logFilePath, true);
                fileHandler.setLevel(Level.ALL);
                logger.addHandler(fileHandler);
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error during file log configuration", e);
            }
        }
    }

    public static void error(String message) {
        logger.severe(message);
    }

    public static void info(String message) {
        logger.info(message);
    }

}

