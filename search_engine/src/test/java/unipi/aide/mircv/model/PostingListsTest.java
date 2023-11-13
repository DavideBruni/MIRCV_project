package unipi.aide.mircv.model;


import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Collectors;

class PostingListsTest {

    @Test
    void addSinglePosting() {
        PostingLists postingLists = new PostingLists();
        long docId = 1;
        String token = "exampleToken";
        int frequency = 2;

        postingLists.add(docId, token, frequency);

        assertTrue(postingLists.postings.containsKey(token));
        assertEquals(1, postingLists.postings.get(token).size());
        assertEquals(docId, postingLists.postings.get(token).get(0).getDocid());
        assertEquals(frequency, postingLists.postings.get(token).get(0).getFrequency());
    }

    @Test
    void addMultiplePostings() {
        PostingLists postingLists = new PostingLists();
        String token = "exampleToken";
        List<Long> docIds = Arrays.asList(1L, 2L, 3L);
        List<Integer> frequencies = Arrays.asList(2, 3, 1);

        for(int i = 0; i<3; i++) {
            postingLists.add(docIds.get(i), token, frequencies.get(i));
        }

        assertTrue(postingLists.postings.containsKey(token));
        assertEquals(3, postingLists.postings.get(token).size());
        // Add more assertions based on the expected values.
    }

    @Test
    void addPostingLists() {
        PostingLists postingLists1 = new PostingLists();
        postingLists1.add(1, "token1", 2);
        postingLists1.add(2, "token1", 3);

        PostingLists postingLists2 = new PostingLists();
        postingLists2.add(1, "token1", 1);
        postingLists2.add(3, "token1", 4);

        PostingLists postingLists3 = new PostingLists();
        postingLists3.add(Arrays.asList(postingLists1, postingLists2), "token1");

        assertTrue(postingLists3.postings.containsKey("token1"));
        assertEquals(4, postingLists3.postings.get("token1").size());
        // Add more assertions based on the expected values.
    }

    @Test
    void sortPostings() {
        PostingLists postingLists = new PostingLists();
        postingLists.add(2, "token2", 3);
        postingLists.add(1, "token1", 2);
        postingLists.add(3, "token3", 1);

        postingLists.sort();

        // Use LinkedHashMap to maintain the order.
        Map<String, List<Posting>> sortedPostings = postingLists.postings.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new)
                );

        assertEquals(sortedPostings, postingLists.postings);
    }

        @Mock
        private Lexicon lexicon;

        @Test
        void readWriteTestNotCompressedFirstList() {
            MockitoAnnotations.initMocks(this);

            PostingLists originalPostingLists = new PostingLists();
            originalPostingLists.add(1, "token1", 2);
            originalPostingLists.add(2, "token1", 3);
            originalPostingLists.add(1, "token2", 1);

            // Writing to disk
            originalPostingLists.writeToDisk(false, lexicon);

            // Reading from disk
            PostingLists readPostingLists = new PostingLists();
            readPostingLists = readPostingLists.readFromDisk("token1", 0, 0, 0, 2, false);
            PostingLists readPostingLists_token2 = new PostingLists();
            readPostingLists_token2 = readPostingLists_token2.readFromDisk("token2", 0, 16, 8, 1, false);
            readPostingLists.add(readPostingLists_token2.getPostingList("token2").get(0).getDocid(), "token2", readPostingLists_token2.getPostingList("token2").get(0).getFrequency());


            // Assertions
            assertPostingListsEqual(originalPostingLists, readPostingLists);
        }


    @Test
    void readWriteTestNotCompressedGenericList() {
        MockitoAnnotations.initMocks(this);

        PostingLists originalPostingLists = new PostingLists();
        originalPostingLists.add(5, "token1", 2);
        originalPostingLists.add(10, "token1", 3);
        originalPostingLists.add(11, "token2", 1);

        // Writing to disk
        originalPostingLists.writeToDisk(false, lexicon);

        // Reading from disk
        PostingLists readPostingLists = new PostingLists();
        readPostingLists = readPostingLists.readFromDisk("token1", 0, 0, 0, 2, false);
        PostingLists readPostingLists_token2 = new PostingLists();
        readPostingLists_token2 = readPostingLists_token2.readFromDisk("token2", 0, 16, 8, 1, false);
        readPostingLists.add(readPostingLists_token2.getPostingList("token2").get(0).getDocid(), "token2", readPostingLists_token2.getPostingList("token2").get(0).getFrequency());


        // Assertions
        assertPostingListsEqual(originalPostingLists, readPostingLists);
    }



    @Test
        void readWriteTestCompressed() {
            MockitoAnnotations.initMocks(this);

            PostingLists originalPostingLists = new PostingLists();
            originalPostingLists.add(1, "token1", 2);
            originalPostingLists.add(2, "token1", 3);
            originalPostingLists.add(3, "token1", 1);

            LexiconEntry lexiconEntry = new LexiconEntry();
            lexicon = new Lexicon();
            lexiconEntry.setDocIdOffset(0);
            lexiconEntry.setFrequencyOffset(0);
            lexiconEntry.setNumBlocks(0);
            lexiconEntry.setSkipPointerOffset(0);
            lexicon.setEntry("token1",lexiconEntry);
            // Writing to disk
            originalPostingLists.writeToDisk(true, lexicon);

            // Reading from disk
            PostingLists readPostingLists = new PostingLists();
            readPostingLists = readPostingLists.readFromDisk("token1", 0, 0, 0, 3, true);

            // Assertions
            assertPostingListsEqual(originalPostingLists, readPostingLists);
        }

        private void assertPostingListsEqual(PostingLists expected, PostingLists actual) {
            // Add assertions based on expected and actual values
            assertEquals(expected.postings.size(), actual.postings.size());
            for (String token : expected.postings.keySet()) {
                assertTrue(actual.postings.containsKey(token));
                List<Posting> expectedPostings = expected.postings.get(token);
                List<Posting> actualPostings = actual.postings.get(token);
                assertEquals(expectedPostings.size(), actualPostings.size());
                for (int i = 0; i < expectedPostings.size(); i++) {
                    Posting expectedPosting = expectedPostings.get(i);
                    Posting actualPosting = actualPostings.get(i);
                    assertEquals(expectedPosting.getDocid(), actualPosting.getDocid());
                    assertEquals(expectedPosting.getFrequency(), actualPosting.getFrequency());
                }
            }
        }


}
