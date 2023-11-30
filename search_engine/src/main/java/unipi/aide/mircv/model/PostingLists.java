package unipi.aide.mircv.model;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.PostingListStoreException;
import unipi.aide.mircv.helpers.StreamHelper;
import unipi.aide.mircv.log.CustomLogger;
import unipi.aide.mircv.queryProcessor.Scorer;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class PostingLists {

    Map<String, PostingList> postings;                  // for each token, we have it's posting list
    private static final String TEMP_DOC_ID_DIR = "/invertedIndex/temp/docIds";
    private static final String TEMP_FREQ_DIR ="/invertedIndex/temp/frequencies";
    private static int NUM_FILE_WRITTEN = 0;       // since partial posting lists are stored in different partition, we need to know how many of them
    private static final int POSTING_SIZE_THRESHOLD = 2048;      //2KB

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
        Posting posting = new Posting(docId, frequency);
        if (!postings.containsKey(token)){
            PostingList postingList = new PostingList(){};
            postingList.add(posting);
            postings.put(token,postingList);     // If the specified token is not present in the postings, a new PostingList is created.
        }else {
            // add the new Posting to the token posting list
            postings.get(token).add(posting);
        }
    }

    /**
     * Merges multiple PostingLists associated with the same token into a single PostingList.
     *
     * @param postingLists A list of PostingLists to be merged.
     * @param token        The token associated with the PostingList.
     */
    public static PostingList merge(List<PostingList> postingLists, String token) {
        List<Posting> postings_merged = new ArrayList<>();
        for(PostingList postingList : postingLists){
            postings_merged.addAll(postingList.postingList);
        }
        // Sort the merged PostingList in ascending order
        postings_merged.sort(Comparator.comparingInt(Posting::getDocid));
        // The resulting PostingList is then associated with the specified token
        return new PostingList(postings_merged,token);
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


    /**
     * Writes the posting lists to disk, storing document IDs and frequencies in separate files.
     * The method supports both compressed and uncompressed formats based on the specified parameter.
     *
     * @param compressed If true, the data is written in a compressed format; otherwise, it is uncompressed.
     */
    public void writeToDisk(boolean compressed) throws PostingListStoreException {
        // creates the directories if needed
        StreamHelper.createDir(Configuration.getRootDirectory() + TEMP_DOC_ID_DIR);
        StreamHelper.createDir(Configuration.getRootDirectory() + TEMP_FREQ_DIR);

        try (FileOutputStream docStream = new FileOutputStream(Configuration.getRootDirectory() + TEMP_DOC_ID_DIR + "/part" + NUM_FILE_WRITTEN + ".dat");
             FileOutputStream freqStream = new FileOutputStream(Configuration.getRootDirectory() + TEMP_FREQ_DIR + "/part" + NUM_FILE_WRITTEN + ".dat")){
            if (!compressed){
                int offset = 0;
                try(DataOutputStream docStream_dos = new DataOutputStream(docStream);
                    DataOutputStream freqStream_dos = new DataOutputStream(freqStream)) {
                    Set<String> keySet = postings.keySet();
                    for(String token : keySet)
                        offset = postings.get(token).writeToDiskNotCompressed(docStream_dos, freqStream_dos, offset, false, Lexicon.getEntry(token,false));
                }
            }else{
                int [] offsets = new int[2];
                Set<String> keySet = postings.keySet();
                for(String token : keySet)
                    offsets = postings.get(token).writeToDiskCompressed(docStream, freqStream,offsets[0], offsets[1], false, Lexicon.getEntry(token, false));
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
