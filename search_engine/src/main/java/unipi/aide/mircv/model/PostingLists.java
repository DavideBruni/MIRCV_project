package unipi.aide.mircv.model;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.PostingListStoreException;
import unipi.aide.mircv.helpers.StreamHelper;
import unipi.aide.mircv.log.CustomLogger;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

public class PostingLists {

    Map<String, PostingList> postings;                  // for each token, we have it's posting list
    private static final String TEMP_DOC_ID_DIR = "/invertedIndex/temp/docIds";
    private static final String TEMP_FREQ_DIR ="/invertedIndex/temp/frequencies";
    private static int NUM_FILE_WRITTEN = 0;       // since partial posting lists are stored in different partition, we need to know how many of them

    public PostingLists() { this.postings = new HashMap<>(1500, 0.75F); }

    /**
     * Adds a posting to the inverted index of a given token.
     * The Posting containing the provided document ID and frequency is then added to the PostingList.
     *
     * @param docId     The document ID to the posting list.
     * @param token     The token of the posting list.
     * @param frequency The frequency of the token in the document.
     */
    public void add(int docId, String token, int frequency) {
        if (!postings.containsKey(token)){
            PostingList postingList = new UncompressedPostingList();
            postingList.add(docId,frequency);
            postings.put(token,postingList);     // If the specified token is not present in the postings, a new PostingList is created.
        }else {
            // add the new Posting to the token posting list
            postings.get(token).add(docId,frequency);
        }
    }

    public void sort() {
        postings = new LinkedHashMap<>(
                postings.entrySet()
                        .stream()
                        .sorted(Map.Entry.comparingByKey())
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue,
                                        (e1, e2) -> e1, LinkedHashMap::new)
                        )
        );
    }

    public void writeOnDisk(boolean compressed) throws PostingListStoreException {
        // creates the directories if needed
        StreamHelper.createDir(Configuration.getRootDirectory() + TEMP_DOC_ID_DIR);
        StreamHelper.createDir(Configuration.getRootDirectory() + TEMP_FREQ_DIR);

        try (FileChannel docStream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getRootDirectory() + TEMP_DOC_ID_DIR + "/part" + NUM_FILE_WRITTEN + ".dat"),
                StandardOpenOption.APPEND,
                StandardOpenOption.CREATE);
             FileChannel freqStream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getRootDirectory() + TEMP_FREQ_DIR + "/part" + NUM_FILE_WRITTEN + ".dat"),
                     StandardOpenOption.APPEND,
                     StandardOpenOption.CREATE)){
            int [] offsets = new int[2];
            Set<String> keySet = postings.keySet();
            for(String token : keySet) {
                PostingList postingList = postings.get(token);
                if (compressed){
                    postingList = new CompressedPostingList(postingList);
                }
                LexiconEntry lexiconEntry = Lexicon.getEntry(token,false);
                lexiconEntry.setDocIdOffset(offsets[0]);
                lexiconEntry.setFrequencyOffset(offsets[1]);
                offsets = postingList.writeOnDisk(docStream, freqStream, offsets);
                // it's not necessary add the docIds and frequencies size since we have no block division here
            }
            NUM_FILE_WRITTEN++;

        } catch (IOException e) {
            CustomLogger.error("Error while writing posting lists on disk: abort operation");
            throw new PostingListStoreException();
        }

    }

    @Override
    public String toString() {
        return "PostingLists{" +
                "postings=" + postings +
                '}';
    }
}
