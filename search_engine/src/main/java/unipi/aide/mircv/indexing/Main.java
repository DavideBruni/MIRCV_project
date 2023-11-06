package unipi.aide.mircv.indexing;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import unipi.aide.mircv.model.InvertedIndex;

import java.io.*;
import java.nio.file.Path;

public class Main {

    private static final String TSV_FILE_NAME = "collection.tsv";

    public static void main( String[] args ){
        if (args.length < 2 ) {
            // Error in input parameters, write log
            System.exit(-1);
        }

        Path filePath = Path.of(args[0]);

        InvertedIndex invertedIndex;


        // Read from compressed file and automatic handle file closure
        try (FileInputStream fis = new FileInputStream(filePath.toFile());
             TarArchiveInputStream tarIn = new TarArchiveInputStream(new GzipCompressorInputStream(fis))) {
            TarArchiveEntry entry;
            while ((entry = tarIn.getNextTarEntry()) != null) {     //read each file of the tar.gz archive
                // if I have different files, the next if need to be changed!
                if (entry.getName().equals(TSV_FILE_NAME)) {          // searching for the file with the name TSV_FILE_NAME
                    invertedIndex = new InvertedIndex(tarIn, Boolean.parseBoolean(args[1]), Boolean.FALSE);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(-2);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-2);
        }

    }

}
