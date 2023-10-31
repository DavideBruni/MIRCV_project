package unipi.aide.mircv.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import unipi.aide.mircv.exceptions.DocumentNotFoundException;

import static org.junit.jupiter.api.Assertions.*;

public class DocumentIndexTest {

    private DocumentIndex documentIndex;

    @Test
    void testAddAndRetrieveDocument() {
        long docid = 1;
        String docno = "doc001";
        int docLen = 100;

        // Aggiungi un documento all'indice
        assertDoesNotThrow(() -> {
            DocumentIndex.add(docid, docno, docLen);
        });

        // Recupera il documento dall'indice
        assertDoesNotThrow(() -> {
            DocumentInfo retrievedDoc = DocumentIndex.retrieve(docno);
            assertEquals(docid, retrievedDoc.getDocid());
            assertEquals(docLen, retrievedDoc.getDocLen());
        });
    }

    @Test
    void testRetrieveNonExistentDocument() {
        String nonExistentDocno = "nonexistent001";

        // Cerca un documento che non dovrebbe esistere nell'indice
        assertThrows(DocumentNotFoundException.class, () -> {
            DocumentIndex.retrieve(nonExistentDocno);
        });
    }
}
