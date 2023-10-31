package unipi.aide.mircv.model;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class PostingList {

    private Map<String, List<Posting>> postings;
    private static String docIdPath ="/data/temp/docIds/part";
    private static String freqPath ="/data/temp/docIds/part";
    private static int NUM_FILE_WRITTEN = 0;

    public PostingList() {
        this.postings = new HashMap<>();
    }

    public void add(long docId, String token, int frequency) {
        if (! postings.containsKey(token)){
            postings.put(token, new ArrayList<>());
        }
        postings.get(token).add(new Posting(docId,frequency));
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

    public void writeToDisk(boolean compressed) {
        if (!compressed){
            try (DataOutputStream docStream = new DataOutputStream(new FileOutputStream(docIdPath+ NUM_FILE_WRITTEN));
                 DataOutputStream freqStream = new DataOutputStream(new FileOutputStream(freqPath+ NUM_FILE_WRITTEN))){
                int i = 0;
                for(String key : postings.keySet()) {
                    List<Posting> postingLists = postings.get(key);
                    Lexicon.getInstance().updateDocIdOffset(i*4);
                    Lexicon.getInstance().updateFrequencyOffset(i*4);
                    for (Posting posting : postingLists) {
                        docStream.writeLong(posting.docid);
                        freqStream.writeInt(posting.frequency);
                        i++;
                    }
                }
                NUM_FILE_WRITTEN++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private class Posting{
        long docid;
        int frequency;

        public Posting(long docid, int frequency) {
            this.docid = docid;
            this.frequency = frequency;
        }
    }
}
