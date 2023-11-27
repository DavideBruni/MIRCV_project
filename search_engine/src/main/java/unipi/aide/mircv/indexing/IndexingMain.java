package unipi.aide.mircv.indexing;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.log.CustomLogger;
import unipi.aide.mircv.model.InvertedIndex;

import java.io.*;
import java.nio.file.Path;
import java.time.Duration;
import java.time.format.DateTimeFormatter;

public class IndexingMain {

    private static final String TSV_FILE_NAME = "collection.tsv";

    public static void main( String[] args ){
        // args must be: collection file path, parse (boolean), compressed (boolean), logDirectory
        if (args.length < 3 ) {
            System.err.println("Error in input parameters, parameters are:");
            System.err.println("< Collection file path > String, file must be tar.gz compressed");
            System.err.println("< parse document >       Boolean");
            System.err.println("< compress index >       Boolean");
            System.err.println("< path to log dir >      String, optional");
            System.exit(-1);
        }
        if (args.length > 3 )
            CustomLogger.configureFileLogger(args[3]);
        Configuration.setCOMPRESSED(Boolean.parseBoolean(args[2]));
        Path filePath = Path.of(args[0]);
        boolean parse = Boolean.parseBoolean(args[1]);
        Configuration.setUpPaths("data");

        // Read from compressed file and automatic handle file closure
        try (FileInputStream fis = new FileInputStream(filePath.toFile());
            TarArchiveInputStream tarIn = new TarArchiveInputStream(new GzipCompressorInputStream(fis))) {
            TarArchiveEntry entry;
            while ((entry = tarIn.getNextTarEntry()) != null) {     //read each file of the tar.gz archive
                // supposition: we have only one file, if not, the following if and the variable TSV_FILE_NAME must be changed
                if (entry.getName().equals(TSV_FILE_NAME)) {        // searching for the file with the name TSV_FILE_NAME
                    // Get the current timestamp in milliseconds
                    long timestamp_start = System.currentTimeMillis();
                    InvertedIndex.createInvertedIndex(tarIn, parse, false);
                    long timestamp_stop = System.currentTimeMillis();
                    CustomLogger.info("Index created in: "+(timestamp_stop-timestamp_start)+" milliseconds");
                    Duration duration = Duration.ofMillis(timestamp_stop-timestamp_start);

                    // Format the duration as "HH:mm:ss:SSS"
                    String formattedTime = String.format("%02d:%02d:%02d:%03d",duration.toHoursPart(), duration.toMinutesPart(), duration.toSecondsPart(), duration.toMillisPart());
                    CustomLogger.info(formattedTime);
                }
            }
        } catch (FileNotFoundException e) {
            CustomLogger.error("File "+args[0]+" not found");
            System.exit(-2);
        } catch (IOException e) {
            CustomLogger.error("Error in invertedIndex creation, search engine not usable");
            System.exit(-2);
        }

    }

}
