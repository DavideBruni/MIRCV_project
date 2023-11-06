package unipi.aide.mircv.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

    public class LexiconTest {

        @Test
        public void testReadWriteLexicon() {
            Lexicon lexicon = new Lexicon();

            // Aggiungi alcune voci al lessico
            lexicon.add("apple");
            lexicon.add("banana");
            lexicon.add("cherry");

            // Scrivi il lessico su disco
            lexicon.writeToDisk(true);

            // Leggi il lessico da disco
            Lexicon readLexicon = Lexicon.readFromDisk(-1);

            // Verifica che le voci siano state correttamente scritte e lette
            assertTrue(readLexicon.contains("apple"));
            assertTrue(readLexicon.contains("banana"));
            assertTrue(readLexicon.contains("cherry"));

            // Verifica che le voci abbiano valori corretti
            LexiconEntry appleEntry = readLexicon.getEntry("apple");
            assertNotNull(appleEntry);
            assertEquals(1, appleEntry.getDf());

        }

        @Test
        public void testAddAndGetEntryAtPointer() {
            Lexicon lexicon = new Lexicon();

            // Aggiungi alcune voci al lessico utilizzando il metodo 'add'
            LexiconEntry appleEntry = new LexiconEntry();
            LexiconEntry bananaEntry = new LexiconEntry();
            LexiconEntry cherryEntry = new LexiconEntry();
            lexicon.add("apple", appleEntry);
            lexicon.add("banana", bananaEntry);
            lexicon.add("cherry", cherryEntry);

            // Verifica che le voci siano state correttamente aggiunte
            assertTrue(lexicon.contains("apple"));
            assertTrue(lexicon.contains("banana"));
            assertTrue(lexicon.contains("cherry"));

            // Verifica che i riferimenti alle voci siano corretti
            assertSame(appleEntry, lexicon.getEntry("apple"));
            assertSame(bananaEntry, lexicon.getEntry("banana"));
            assertSame(cherryEntry, lexicon.getEntry("cherry"));

            // Test per il metodo 'getEntryAtPointer'
            assertEquals("apple", lexicon.getEntryAtPointer(0));
            assertEquals("banana", lexicon.getEntryAtPointer(1));
            assertEquals("cherry", lexicon.getEntryAtPointer(2));

            // Test per il comportamento quando il puntatore è fuori dai limiti
            assertNull(lexicon.getEntryAtPointer(3));
        }
    }


