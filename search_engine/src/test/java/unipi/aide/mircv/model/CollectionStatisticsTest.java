package unipi.aide.mircv.model;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.File;
import static org.junit.jupiter.api.Assertions.*;

public class CollectionStatisticsTest {

    private static final String COLLECTION_STATISTICS_PATH = "data/collectionStatistics.dat";

    @BeforeEach
    public void setUp() {
        // Imposta lo stato iniziale prima di ciascun test
        CollectionStatistics.updateCollectionSize();
        CollectionStatistics.updateDocumentsLen(100); // Ad esempio, 100 come dimensione dei documenti
    }

    @AfterEach
    public void tearDown() {
        // Ripristina lo stato iniziale dopo ciascun test
        CollectionStatistics.updateCollectionSize();
        CollectionStatistics.updateDocumentsLen(0);
    }

    @Test
    public void testUpdateCollectionSize() {
        assertEquals(1, CollectionStatistics.getCollectionSize()); // Il test dovrebbe incrementare la dimensione della collezione
    }

    @Test
    public void testUpdateDocumentsLen() {
        assertEquals(100, CollectionStatistics.getDocumentsLen()); // Verifica se la lunghezza dei documenti è stata aggiornata
    }

    @Test
    public void testWriteAndReadFromDisk() {
        // Scrivi i dati su disco
        CollectionStatistics.writeToDisk();

        // Leggi i dati da disco
        CollectionStatistics.readFromDisk();

        // Verifica che i dati siano stati scritti e letti correttamente
        assertEquals(1, CollectionStatistics.getCollectionSize());
        assertEquals(100, CollectionStatistics.getDocumentsLen());

        // Pulisci il file di statistiche dopo aver verificato
        File file = new File(COLLECTION_STATISTICS_PATH);
        if (file.exists()) {
            file.delete();
        }
    }
}
