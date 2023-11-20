package unipi.aide.mircv.model;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.MissingCollectionStatisticException;

import java.io.File;
import static org.junit.jupiter.api.Assertions.*;

class CollectionStatisticsTest {

    @BeforeAll
    static void setTestRootPath(){
        Configuration.setUpPaths("data/test");
    }

    @BeforeEach
    void setUp() {
        // Reset the state before each test
        CollectionStatistics.reset(); // Add a method to reset or reinitialize the state
    }

    @Test
    void testWriteAndReadFromDisk() {
        // Initialize the statistics
        CollectionStatistics.updateCollectionSize();
        CollectionStatistics.updateDocumentsLen(100);
        CollectionStatistics.writeToDisk();

        // Clear the statistics
        CollectionStatistics.updateCollectionSize();
        CollectionStatistics.updateDocumentsLen(0);

        // Read from disk
        try {
            CollectionStatistics.readFromDisk();
        } catch (MissingCollectionStatisticException e) {
            throw new RuntimeException(e);
        }

        // Check if the values were restored correctly
        assertEquals(1, CollectionStatistics.getCollectionSize());
        assertEquals(100, CollectionStatistics.getDocumentsLen());
    }

    @Test
    void testReadWriteConsistency() {
        // Initialize the statistics
        CollectionStatistics.updateCollectionSize();
        CollectionStatistics.updateDocumentsLen(150);
        CollectionStatistics.writeToDisk();

        // Clear the statistics
        CollectionStatistics.updateCollectionSize();
        CollectionStatistics.updateDocumentsLen(0);

        // Modify the values
        CollectionStatistics.updateCollectionSize();
        CollectionStatistics.updateDocumentsLen(200);

        // Read from disk
        try {
            CollectionStatistics.readFromDisk();
        } catch (MissingCollectionStatisticException e) {
            throw new RuntimeException(e);
        }

        // Check if the values were restored correctly
        assertEquals(1, CollectionStatistics.getCollectionSize());
        // The test checks for consistency, so it should still be the original value
        assertEquals(150, CollectionStatistics.getDocumentsLen());
    }

    @Test
    void testReadWriteEmptyFile() {
        // Ensure the file is empty before reading
        File file = new File(Configuration.getCollectionStatisticsPath());
        if (file.exists()) {
            file.delete();
        }

        assertThrows(MissingCollectionStatisticException.class,() -> CollectionStatistics.readFromDisk());

        // Check if the values are initialized to default (0)
        assertEquals(0, CollectionStatistics.getCollectionSize());
        assertEquals(0, CollectionStatistics.getDocumentsLen());
    }
}
