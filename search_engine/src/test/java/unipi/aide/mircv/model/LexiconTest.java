package unipi.aide.mircv.model;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class LexiconTest {

    private Lexicon lexicon;

    @Before
    public void setUp() {
        lexicon = Lexicon.getInstance();
        lexicon.add("word1");
        lexicon.add("word2");
        lexicon.updateDf("word1");
        lexicon.updateDf("word2");
        lexicon.updateDocIdOffset("word1", 100);
        lexicon.updateDocIdOffset("word2", 200);
        lexicon.updateFrequencyOffset("word1", 1000);
        lexicon.updateFrequencyOffset("word2", 2000);
    }

    @Test
    public void testContains() {
        assertTrue(lexicon.contains("word1"));
        assertTrue(lexicon.contains("word2"));
        assertFalse(lexicon.contains("word3"));
    }

    @Test
    public void testUpdateDf() {
        assertEquals(2, lexicon.getEntry("word1").getDf());
        assertEquals(2, lexicon.getEntry("word2").getDf());
    }

    @Test
    public void testUpdateDocIdOffset() {
        assertEquals(100, lexicon.getEntry("word1").getDocIdOffset());
        assertEquals(200, lexicon.getEntry("word2").getDocIdOffset());
    }

    @Test
    public void testUpdateFrequencyOffset() {
        assertEquals(1000, lexicon.getEntry("word1").getFrequencyOffset());
        assertEquals(2000, lexicon.getEntry("word2").getFrequencyOffset());
    }

    @Test
    public void testWriteAndReadFromDisk() {
        try {
            lexicon.writeToDisk();

            // Read the data from the written file
            Map<String, List<LexiconEntry>> readLexicon = Lexicon.readFromDisk();

            // assertEquals(lexicon.entries.size(), readLexicon.entries.size());

            for (String key : readLexicon.keySet()) {
                assertTrue(lexicon.contains(key));
                LexiconEntry originalEntry = lexicon.getEntry(key);
                LexiconEntry readEntry = readLexicon.get(key).get(0);

                // Compare the values
                assertEquals(originalEntry.getDf(), readEntry.getDf());
                assertEquals(originalEntry.getIdf(), readEntry.getIdf());
                assertEquals(originalEntry.getDocIdOffset(), readEntry.getDocIdOffset());
                assertEquals(originalEntry.getFrequencyOffset(), readEntry.getFrequencyOffset());
                assertEquals(originalEntry.getNumBlocks(), readEntry.getNumBlocks());
            }
        } catch (Exception e) {
            fail("Exception occurred: " + e.getMessage());
        } finally {
            // TODO Clean up: Delete the temporary file

        }
    }
}
