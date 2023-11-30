package unipi.aide.mircv.model;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.PostingListStoreException;
import unipi.aide.mircv.log.CustomLogger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PostingListsTest {

    private PostingLists postingLists;

    @BeforeAll
    static void setTestRootPath(){
        Configuration.setUpPaths("data/test");
    }

    @BeforeEach
    void setUp() {
        postingLists = new PostingLists();
        cleanUp();
    }


    private void cleanUp(){
        Path directory = Paths.get(Configuration.getRootDirectory());
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs){
                    return FileVisitResult.CONTINUE; // Do nothing if visiting a file
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc){
                    CustomLogger.error("Error while visiting file "+file); //handle errors
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    if (dir.getFileName().toString().equals("test")) {  //deleting temp dir
                        Files.delete(dir);
                        CustomLogger.info("Directory deleted: " + dir);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            CustomLogger.error("Error while removing temp directories ");
        }
    }

    @Test
    void testAddSinglePosting() {
        int docId = 1;
        String token = "example";
        int frequency = 3;

        postingLists.add(docId, token, frequency);

        assertTrue(postingLists.postings.containsKey(token));
        PostingList postingList = postingLists.postings.get(token);
        assertEquals(1, postingList.getPostingList().size());
        Posting posting = postingList.getPostingList().get(0);
        assertEquals(docId, posting.getDocid());
        assertEquals(frequency, posting.getFrequency());
    }

    @Test
    void testAddMultiplePostings() {
        int docId1 = 1;
        int docId2 = 2;
        String token = "example";
        int frequency1 = 3;
        int frequency2 = 2;

        postingLists.add(docId1, token, frequency1);
        postingLists.add(docId2, token, frequency2);

        assertTrue(postingLists.postings.containsKey(token));
        PostingList postingList = postingLists.postings.get(token);
        assertEquals(2, postingList.getPostingList().size());

        Posting posting1 = postingList.getPostingList().get(0);
        assertEquals(docId1, posting1.getDocid());
        assertEquals(frequency1, posting1.getFrequency());

        Posting posting2 = postingList.getPostingList().get(1);
        assertEquals(docId2, posting2.getDocid());
        assertEquals(frequency2, posting2.getFrequency());
    }

    @Test
    void testAddListOfPostingLists() {
        List<PostingList> postingListsToAdd = new ArrayList<>();
        int docId1 = 1;
        int docId2 = 2;
        String token = "example";
        int frequency1 = 3;
        int frequency2 = 2;

        PostingList postingList1 = new PostingList();
        postingList1.add(new Posting(docId1, frequency1));

        PostingList postingList2 = new PostingList();
        postingList2.add(new Posting(docId2, frequency2));

        postingListsToAdd.add(postingList1);
        postingListsToAdd.add(postingList2);

        //postingLists.add(postingListsToAdd, token);

        assertTrue(postingLists.postings.containsKey(token));
        PostingList mergedPostingList = postingLists.postings.get(token);
        assertEquals(2, mergedPostingList.getPostingList().size());

        Posting mergedPosting1 = mergedPostingList.getPostingList().get(0);
        assertEquals(docId1, mergedPosting1.getDocid());
        assertEquals(frequency1, mergedPosting1.getFrequency());

        Posting mergedPosting2 = mergedPostingList.getPostingList().get(1);
        assertEquals(docId2, mergedPosting2.getDocid());
        assertEquals(frequency2, mergedPosting2.getFrequency());
    }

    @Test
    void testSort() {
        postingLists.add(2, "banana", 3);
        postingLists.add(1, "apple", 2);
        postingLists.add(3, "orange", 1);

        postingLists.sort();

        List<String> sortedTokens = new ArrayList<>(postingLists.postings.keySet());
        assertEquals("apple", sortedTokens.get(0));
        assertEquals("banana", sortedTokens.get(1));
        assertEquals("orange", sortedTokens.get(2));
    }


    @Test
    void testWriteAndReadFromDiskNotCompressed() {
        Configuration.setCOMPRESSED(false);
        Lexicon lexicon = Lexicon.getInstance();
        // Add some postings to the list
        postingLists.add(1, "apple", 3);
        postingLists.add(4, "apple", 3);
        postingLists.add(8, "apple", 3);
        postingLists.add(2, "banana", 2);
        postingLists.add(3, "banana", 2);
        postingLists.add(7, "banana", 2);

        try {
            postingLists.sort();
            postingLists.writeToDisk(false);
            PostingList readFromDisk = new PostingList();
            PostingList appleList = readFromDisk.readFromDisk("apple", 0, 0, 0, 3, false);
            PostingList bananaList = readFromDisk.readFromDisk("banana", 0, 3*8, 3*4, 3, false);

            assertNotNull(appleList);
            assertNotNull(bananaList);

            assertEquals(1, appleList.getPostingList().get(0).getDocid());
            assertEquals(3, appleList.getPostingList().get(0).getFrequency());

            assertEquals(4, appleList.getPostingList().get(1).getDocid());
            assertEquals(3, appleList.getPostingList().get(1).getFrequency());

            assertEquals(8, appleList.getPostingList().get(2).getDocid());
            assertEquals(3, appleList.getPostingList().get(2).getFrequency());

            assertEquals(2, bananaList.getPostingList().get(0).getDocid());
            assertEquals(2, bananaList.getPostingList().get(0).getFrequency());

            assertEquals(3, bananaList.getPostingList().get(1).getDocid());
            assertEquals(2, bananaList.getPostingList().get(1).getFrequency());

            assertEquals(7, bananaList.getPostingList().get(2).getDocid());
            assertEquals(2, bananaList.getPostingList().get(2).getFrequency());

        } catch (PostingListStoreException e) {
            // Handle the exception, if needed
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }


    // execute only this, for some reason if execute all together this test fails; probably is something on concurrency

    @Test
    void testWriteAndReadFromDiskCompressed() {
        Configuration.setCOMPRESSED(true);
        Lexicon lexicon = Lexicon.getInstance();
        // Add some postings to the list
        postingLists.add(1, "apple", 3);
        postingLists.add(4, "apple", 3);
        postingLists.add(8, "apple", 3);
        postingLists.add(2, "banana", 2);
        postingLists.add(3, "banana", 2);
        postingLists.add(7, "banana", 2);
        Lexicon.setEntry("apple",new LexiconEntry());
        Lexicon.setEntry("banana",new LexiconEntry());

        try {
            postingLists.sort();
            postingLists.writeToDisk(true);

            PostingList readFromDisk = new PostingList();
            PostingList appleList = readFromDisk.readFromDisk("apple", 0, 0, 0, 3, true);
            PostingList bananaList = readFromDisk.readFromDisk("banana", 0, 15, 3, 3, true);

            assertNotNull(appleList);
            assertNotNull(bananaList);

            assertEquals(1, appleList.getPostingList().get(0).getDocid());
            assertEquals(3, appleList.getPostingList().get(0).getFrequency());

            assertEquals(4, appleList.getPostingList().get(1).getDocid());
            assertEquals(3, appleList.getPostingList().get(1).getFrequency());

            assertEquals(8, appleList.getPostingList().get(2).getDocid());
            assertEquals(3, appleList.getPostingList().get(2).getFrequency());

            assertEquals(2, bananaList.getPostingList().get(0).getDocid());
            assertEquals(2, bananaList.getPostingList().get(0).getFrequency());

            assertEquals(3, bananaList.getPostingList().get(1).getDocid());
            assertEquals(2, bananaList.getPostingList().get(1).getFrequency());

            assertEquals(7, bananaList.getPostingList().get(2).getDocid());
            assertEquals(2, bananaList.getPostingList().get(2).getFrequency());

        } catch (PostingListStoreException e) {
            // Handle the exception, if needed
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
