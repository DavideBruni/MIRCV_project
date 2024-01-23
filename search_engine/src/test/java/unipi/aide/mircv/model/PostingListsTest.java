package unipi.aide.mircv.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PostingListsTest {

    @Test
    void add() {
        PostingLists postingLists = new PostingLists();
        postingLists.add(1, "token1", 3);
        postingLists.add(2, "token1", 2);
        postingLists.add(1, "token2", 1);

        assertEquals(3, ((UncompressedPostingList)postingLists.postings.get("token1")).frequencies.get(0));
        assertEquals(2, ((UncompressedPostingList)postingLists.postings.get("token1")).frequencies.get(1));

        assertEquals(1, ((UncompressedPostingList)postingLists.postings.get("token2")).frequencies.get(0));
    }

    @Test
    void sortTest() {
        PostingLists postingLists = new PostingLists();
        postingLists.add(1, "token3", 3);
        postingLists.add(2, "token2", 2);
        postingLists.add(1, "token1", 1);

        // Verifica che i token siano in ordine alfabetico
        postingLists.sort();
        assertEquals("token1", postingLists.postings.keySet().toArray()[0]);
        assertEquals("token2", postingLists.postings.keySet().toArray()[1]);
        assertEquals("token3", postingLists.postings.keySet().toArray()[2]);
    }
}