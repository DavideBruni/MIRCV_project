package unipi.aide.mircv.model;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import unipi.aide.mircv.configuration.Configuration;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SkipPointerTest {

    @BeforeAll
    static void setTestRootPath(){
        Configuration.setUpPaths("data/test");
    }

    @BeforeEach
    void setUp() {
        // Clean up before each test
        File file = new File(Configuration.getSkipPointersPath());
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    void testWriteAndReadSkipPointers() {
        // Create some test skip pointers
        List<SkipPointer> skippingPointers = new ArrayList<>();
        skippingPointers.add(new SkipPointer(100, 50, 25, 10));
        skippingPointers.add(new SkipPointer(200, 75, 35, 15));

        // Create a test lexicon entry
        LexiconEntry lexiconEntry = new LexiconEntry();

        // Write skip pointers to file
        int numBlocksWritten = SkipPointer.write(skippingPointers, lexiconEntry);

        // Check if the number of skip pointers written matches the expected value
        assertEquals(skippingPointers.size(), numBlocksWritten);

        // Check if the skip pointer offset in the lexicon entry is set correctly
        assertEquals(0, lexiconEntry.getSkipPointerOffset());

        // Read skip pointers from file and verify
        List<SkipPointer> readSkipPointers = readSkipPointersFromFile(Configuration.getSkipPointersPath());

        // Check if the read skip pointers match the original ones
        assertEquals(skippingPointers.size(), readSkipPointers.size());
        for (int i = 0; i < skippingPointers.size(); i++) {
            SkipPointer originalSkipPointer = skippingPointers.get(i);
            SkipPointer readSkipPointer = readSkipPointers.get(i);
            assertEquals(originalSkipPointer.getMaxDocId(), readSkipPointer.getMaxDocId());
            assertEquals(originalSkipPointer.getDocIdsOffset(), readSkipPointer.getDocIdsOffset());
            assertEquals(originalSkipPointer.getFrequencyOffset(), readSkipPointer.getFrequencyOffset());
            assertEquals(originalSkipPointer.getNumDocId(), readSkipPointer.getNumDocId());
        }
    }

    private List<SkipPointer> readSkipPointersFromFile(String filePath) {
        List<SkipPointer> skipPointers = new ArrayList<>();
        int i = 0;
        while(true){
            try {
                skipPointers.add(SkipPointer.readFromDisk(0, i));
                i++;
            } catch (IOException e) {
                break;
            }
        }
        return skipPointers;
    }
}
