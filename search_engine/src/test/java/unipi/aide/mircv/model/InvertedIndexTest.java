package unipi.aide.mircv.model;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import unipi.aide.mircv.configuration.Configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;

/**
 *
 * This class doesn't perform unitTest, but are small test for invertedIndex creation, results are displayed on stdout
 *
 */

public class InvertedIndexTest {

    @AfterEach
    void cleanUp(){
   //     StreamHelper.deleteDir(Paths.get("data"));
    }

    @Test
    void smallIndexOneBlockNotCompressed(){
        // see stdout
        String TSV_FILE_NAME = "collection.tsv";
        Path filePath = Path.of("utils/toyCollection.tar.gz");
        Configuration.setUpPaths("toyScraper/data");
        try (FileInputStream fis = new FileInputStream(filePath.toFile());
             TarArchiveInputStream tarIn = new TarArchiveInputStream(new GzipCompressorInputStream(fis))) {
            TarArchiveEntry entry;
            while ((entry = tarIn.getNextTarEntry()) != null) {     //read each file of the tar.gz archive
                // supposition: we have only one file, if not, the following if and the variable TSV_FILE_NAME must be changed
                if (entry.getName().equals(TSV_FILE_NAME)) {        // searching for the file with the name TSV_FILE_NAME
                    InvertedIndex.createInvertedIndex(tarIn, true, true);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    @Test
    void smallIndexOneBlockCompressed(){
        // see stdout
        String TSV_FILE_NAME = "collection.tsv";
        Path filePath = Path.of("utils/toyCollection.tar.gz");
        Configuration.setCOMPRESSED(true);
        try (FileInputStream fis = new FileInputStream(filePath.toFile());
             TarArchiveInputStream tarIn = new TarArchiveInputStream(new GzipCompressorInputStream(fis))) {
            TarArchiveEntry entry;
            while ((entry = tarIn.getNextTarEntry()) != null) {     //read each file of the tar.gz archive
                // supposition: we have only one file, if not, the following if and the variable TSV_FILE_NAME must be changed
                if (entry.getName().equals(TSV_FILE_NAME)) {        // searching for the file with the name TSV_FILE_NAME
                    InvertedIndex.createInvertedIndex(tarIn, true, true);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    @Test
    void indexMultipleBlockNotCompressed(){
        // see stdout
        String TSV_FILE_NAME = "collection.tsv";
        Path filePath = Path.of("utils/test_collection.tar.gz");
        try (FileInputStream fis = new FileInputStream(filePath.toFile());
             TarArchiveInputStream tarIn = new TarArchiveInputStream(new GzipCompressorInputStream(fis))) {
            TarArchiveEntry entry;
            while ((entry = tarIn.getNextTarEntry()) != null) {     //read each file of the tar.gz archive
                // supposition: we have only one file, if not, the following if and the variable TSV_FILE_NAME must be changed
                if (entry.getName().equals(TSV_FILE_NAME)) {        // searching for the file with the name TSV_FILE_NAME
                    InvertedIndex.createInvertedIndex(tarIn, true, true);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    @Test
    void indexMultipleBlockCompressed(){
        // see stdout
        Configuration.setUpPaths("data/compressed");
        String TSV_FILE_NAME = "collection.tsv";
        Path filePath = Path.of("utils/test_collection.tar.gz");
        Configuration.setCOMPRESSED(true);
        try (FileInputStream fis = new FileInputStream(filePath.toFile());
             TarArchiveInputStream tarIn = new TarArchiveInputStream(new GzipCompressorInputStream(fis))) {
            TarArchiveEntry entry;
            while ((entry = tarIn.getNextTarEntry()) != null) {     //read each file of the tar.gz archive
                // supposition: we have only one file, if not, the following if and the variable TSV_FILE_NAME must be changed
                if (entry.getName().equals(TSV_FILE_NAME)) {        // searching for the file with the name TSV_FILE_NAME
                    InvertedIndex.createInvertedIndex(tarIn, true, true);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
