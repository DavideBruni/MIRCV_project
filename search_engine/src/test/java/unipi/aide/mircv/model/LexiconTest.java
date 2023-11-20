package unipi.aide.mircv.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import unipi.aide.mircv.exceptions.PartialLexiconNotFoundException;
import unipi.aide.mircv.exceptions.UnableToWriteLexiconException;

import java.io.*;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;

class LexiconTest {

    private Lexicon lexicon;

    @BeforeEach
    void setUp() {
        lexicon = Lexicon.getInstance();
        lexicon.clear(); // Ensure a clean state before each test
    }



    @Test
    void testGetTokens() throws PartialLexiconNotFoundException {
        // Prepare test data
        String[] tokens = new String[]{"token1", null, "token3"};
        DataInputStream[] partialLexiconStreams = new DataInputStream[]{
                createDataInputStream("token1"),
                null,
                createDataInputStream("token3")
        };

        // Call the getTokens method
        String[] updatedTokens = Lexicon.getTokens(tokens, partialLexiconStreams);

        // Assertions
        assertAll(
                () -> assertEquals("token1", updatedTokens[0]),
                () -> assertNull(updatedTokens[1]),
                () -> assertEquals("token3", updatedTokens[2])
        );
    }

    @Test
    void testClear() {
        // Add some entries to the lexicon
        lexicon.add("token1");
        lexicon.add("token2");

        // Call the clear method
        lexicon.clear();

        // Assertions
        assertEquals(0, lexicon.numberOfEntries());
        assertFalse(lexicon.contains("token1"));
        assertFalse(lexicon.contains("token2"));
    }

    @Test
    void testAdd() {
        // Add a token to the lexicon
        lexicon.add("testToken");

        // Assertions
        assertTrue(lexicon.contains("testToken"));
        assertEquals(1, lexicon.numberOfEntries());
    }

    // Helper method to create a DataInputStream for testing
    private DataInputStream createDataInputStream(String token) {
        byte[] tokenBytes = token.getBytes();
        return new DataInputStream(new ByteArrayInputStream(tokenBytes));
    }



    @Test
    void testWriteAndGetEntry() throws UnableToWriteLexiconException, IOException {
        // Prepare test data
        String[] tokens = {"testToken","tokenTest","Gatto","Cane","Mammifero"};
        LexiconEntry lexiconEntry = new LexiconEntry();

        for(String token : tokens)
            lexicon.add(token, lexiconEntry);



        // Call the writeToDisk method
        Lexicon.writeToDisk(false);

        for(String token : tokens)
            assertNotNull(Lexicon.getEntry(token));
    }


    @Test
    void testReadEntry() {
        // Prepare test data
        LexiconEntry lexiconEntry = new LexiconEntry(10, 2.5, 20, 30, 5, 100);
        byte[] entryBytes = createByteArrayFromLexiconEntry(lexiconEntry);

        // Create a DataInputStream from the byte array
        DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(entryBytes));

        // Call the readEntry method
        LexiconEntry readLexiconEntry = Lexicon.readEntry(dataInputStream);

        // Assertions
        assertAll(
                () -> assertNotNull(readLexiconEntry),
                () -> assertEquals(lexiconEntry.getDf(), readLexiconEntry.getDf()),
                () -> assertEquals(lexiconEntry.getIdf(), readLexiconEntry.getIdf()),
                () -> assertEquals(lexiconEntry.getDocIdOffset(), readLexiconEntry.getDocIdOffset()),
                () -> assertEquals(lexiconEntry.getFrequencyOffset(), readLexiconEntry.getFrequencyOffset()),
                () -> assertEquals(lexiconEntry.getNumBlocks(), readLexiconEntry.getNumBlocks()),
                () -> assertEquals(lexiconEntry.getSkipPointerOffset(), readLexiconEntry.getSkipPointerOffset())
        );
    }

    // Helper method to create a byte array from a LexiconEntry
    private byte[] createByteArrayFromLexiconEntry(LexiconEntry lexiconEntry) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
            dataOutputStream.writeInt(lexiconEntry.getDf());
            dataOutputStream.writeDouble(lexiconEntry.getIdf());
            dataOutputStream.writeInt(lexiconEntry.getDocIdOffset());
            dataOutputStream.writeInt(lexiconEntry.getFrequencyOffset());
            dataOutputStream.writeInt(lexiconEntry.getNumBlocks());
            dataOutputStream.writeInt(lexiconEntry.getSkipPointerOffset());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return byteArrayOutputStream.toByteArray();
    }

}
