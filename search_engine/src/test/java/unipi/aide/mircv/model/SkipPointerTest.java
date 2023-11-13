package unipi.aide.mircv.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SkipPointerTest {

    private static final String TEST_SKIP_POINTERS_PATH = "data/skip_pointers.dat";

    @BeforeEach
    void setUp() {
        // Clean up before each test
        File file = new File(TEST_SKIP_POINTERS_PATH);
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    void testWriteSkipPointers() {
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
        List<SkipPointer> readSkipPointers = readSkipPointersFromFile(TEST_SKIP_POINTERS_PATH);

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
        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(filePath))) {
            while (dataInputStream.available() > 0) {
                long maxDocId = dataInputStream.readLong();
                int docIdsOffset = dataInputStream.readInt();
                int frequencyOffset = dataInputStream.readInt();
                int numDocId = dataInputStream.readInt();
                skipPointers.add(new SkipPointer(maxDocId, docIdsOffset, frequencyOffset, numDocId));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return skipPointers;
    }
}
