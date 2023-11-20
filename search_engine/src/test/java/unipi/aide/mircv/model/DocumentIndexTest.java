package unipi.aide.mircv.model;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.DocumentNotFoundException;
import unipi.aide.mircv.exceptions.UnableToAddDocumentIndexException;

import static org.junit.jupiter.api.Assertions.*;

class DocumentIndexTest {
    @BeforeAll
    static void setTestRootPath(){
        Configuration.setUpPaths("data/test");
    }
    @Test
    void testAddAndRetrieveDocument() throws UnableToAddDocumentIndexException, DocumentNotFoundException {
        long docid = 1;
        String docno = "1";
        int docLen = 500;

        // Add a document to the index
        DocumentIndex.add(docid, docno, docLen);

        // Retrieve the document
        DocumentInfo retrievedDoc = DocumentIndex.retrieve(docno);

        // Check if the retrieved document matches the added document
        assertEquals(docid, retrievedDoc.getDocid());
        assertEquals(docno, retrievedDoc.getPid());
        assertEquals(docLen, retrievedDoc.getDocLen());
    }

    @Test
    void testRetrieveNonExistingDocument() {
        String nonExistingDocno = "nonExistingDoc";

        // Attempt to retrieve a non-existing document should throw DocumentNotFoundException
        assertThrows(DocumentNotFoundException.class, () -> DocumentIndex.retrieve(nonExistingDocno));
    }

    @Test
    void testGetDocumentLength() throws UnableToAddDocumentIndexException, DocumentNotFoundException {
        long docid = 456;
        String docno = "456";
        int docLen = 700;

        // Add a document to the index
        DocumentIndex.add(docid, docno, docLen);

        // Retrieve the document length using docid
        int retrievedDocLen = DocumentIndex.getDocumentLength(docid);

        // Check if the retrieved document length matches the added document length
        assertEquals(docLen, retrievedDocLen);
    }

    @Test
    void testGetDocumentLengthForNonExistingDocument() {
        long nonExistingDocid = 789;

        // Attempt to get the length of a non-existing document should throw DocumentNotFoundException
        assertThrows(DocumentNotFoundException.class, () -> DocumentIndex.getDocumentLength(nonExistingDocid));
    }
}
