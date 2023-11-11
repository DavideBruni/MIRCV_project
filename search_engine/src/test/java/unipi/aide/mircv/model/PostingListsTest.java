package unipi.aide.mircv.model;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class PostingListsTest {
    private PostingLists postingLists;

    @Before
    public void setUp() {
        postingLists = new PostingLists();
    }

    @Test
    public void testAddAndReadFromDisk() {
        List<PostingLists> postingLists = new ArrayList<>();

        PostingLists postingLists1 = new PostingLists();
        postingLists1.add(1, "token1", 3);
        postingLists1.add(2, "token1", 2);

        PostingLists postingLists2 = new PostingLists();
        postingLists2.add(2, "token1", 4);
        postingLists2.add(3, "token1", 1);

        postingLists.add(postingLists1);
        postingLists.add(postingLists2);

        int size = this.postingLists.add(postingLists, "token1");
        assertEquals(48, size); // Make sure the size matches the expected value

        PostingLists readPostingLists = this.postingLists.readFromDisk("token1", 0, 0, 0, size, 0);
        assertNotNull(readPostingLists);

    }

    @Test
    public void testAddAndSort() {
        postingLists.add(2, "token2", 4);
        postingLists.add(3, "token3", 1);
        postingLists.add(1, "token1", 3);

        postingLists.sort();

        // Check if the postings are sorted by token name
        List<String> keys = new ArrayList<>(postingLists.postings.keySet());
        assertEquals("token1", keys.get(0));
        assertEquals("token2", keys.get(1));
        assertEquals("token3", keys.get(2));

        assertEquals(1, postingLists.postings.get(keys.get(0)).get(0).docid);
        assertEquals(2, postingLists.postings.get(keys.get(1)).get(0).docid);
        assertEquals(3, postingLists.postings.get(keys.get(2)).get(0).docid);
    }

    /*
    @Test
    public void testAddSkipPointers() {
        Lexicon lexicon = new Lexicon(); // You need to create a Lexicon object for this test
        PostingList.Posting posting1 = postingList.new Posting(1, 3);
        PostingList.Posting posting2 = postingList.new Posting(2, 4);
        PostingList.Posting posting3 = postingList.new Posting(3, 2);

        postingList.postings.put("token3", new ArrayList<>());
        postingList.postings.get("token3").add(posting1);
        postingList.postings.get("token3").add(posting2);
        postingList.postings.get("token3").add(posting3);

        LexiconEntry lexiconEntry = new LexiconEntry(); // You need to create a LexiconEntry object for this test
        postingList.addSkipPointers("token3", lexiconEntry, false);

        // Check if the number of blocks is calculated correctly
        assertEquals(2, lexiconEntry.getNumBlocks());

        // Check if the skip pointers are added to the LexiconEntry
        List<SkipPointer> skipPointers = lexiconEntry.getSkipPointers();
        assertNotNull(skipPointers);
        assertEquals(2, skipPointers.size());
        assertEquals(1, skipPointers.get(0).getDocId());
        assertEquals(2, skipPointers.get(1).getDocId());
    }
     */
}
