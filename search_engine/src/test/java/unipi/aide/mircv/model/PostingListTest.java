package unipi.aide.mircv.model;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class PostingListTest {

    @Test
    public void testAddAndReadFromDisk() {
        PostingList postingList = new PostingList();
        long docId1 = 1L;
        long docId2 = 2L;
        String token = "testToken";
        int frequency1 = 3;
        int frequency2 = 5;

        // Add postings to the PostingList
        postingList.add(docId1, token, frequency1);
        postingList.add(docId2, token, frequency2);

        // Write to disk
        postingList.writeToDisk(false);

        // Read from disk
        PostingList readPostingList = new PostingList();
        List<PostingList.Posting> readPostings = readPostingList.readFromDisk();

        // Verify that the read postings match the ones added
        assertEquals(2, readPostings.size());

        PostingList.Posting readPosting1 = readPostings.get(0);
        assertEquals(docId1, readPosting1.docid);
        assertEquals(frequency1, readPosting1.frequency);

        PostingList.Posting readPosting2 = readPostings.get(1);
        assertEquals(docId2, readPosting2.docid);
        assertEquals(frequency2, readPosting2.frequency);
    }
}

