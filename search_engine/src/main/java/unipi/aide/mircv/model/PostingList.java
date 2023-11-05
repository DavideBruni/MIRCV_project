package unipi.aide.mircv.model;

import unipi.aide.mircv.fileHelper.FileHelper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class PostingList {

    private Map<String, List<Posting>> postings;
    private static String DOC_ID_PATH ="data/temp/docIds";
    private static String FREQ_PATH ="data/temp/frequencies";
    private static int NUM_FILE_WRITTEN = 0;

    public PostingList() {
        this.postings = new HashMap<>();
    }

    private PostingList(String token, List<Posting> postings_merged) {
        this.postings = new HashMap<>();
        postings.put(token,postings_merged);
    }

    public void add(List<PostingList> postingLists, String token) {
        List<Posting> postings_merged = new ArrayList<>();
        for(PostingList postingList : postingLists){        // per lo stesso token devo fare il merge in unica p.l
            postings_merged.addAll(postingList.postings.get(token));
        }
        postings.put(token, postings_merged);               //ottenuta la pl, la associo al token
    }

    public PostingList readFromDisk(String token, int partition, int docIdOffset, int frequencyOffset, int docIdSize, int frequencySize) {
        PostingList res = new PostingList();
        try (DataInputStream docStream = new DataInputStream(new FileInputStream(DOC_ID_PATH + "/part"+partition));
             DataInputStream freqStream = new DataInputStream(new FileInputStream(FREQ_PATH + "/part" + partition))) {
            docStream.skipBytes(docIdOffset);
            freqStream.skipBytes(frequencyOffset);
            for(int i = 0; i< docIdSize/4; i++) {
                try{
                    long docId = docStream.readLong();
                    int frq = freqStream.readInt();
                    res.add(docId,token,frq);
                }catch (EOFException eof){
                    break;
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    public void add(long docId, String token, int frequency) {
        if (!postings.containsKey(token)){
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

    public void writeToDisk(boolean compressed, Lexicon lexicon) {
        FileHelper.createDir(DOC_ID_PATH);
        FileHelper.createDir(FREQ_PATH);
        if (!compressed){
            try (DataOutputStream docStream = new DataOutputStream(new FileOutputStream(DOC_ID_PATH + "/part" + NUM_FILE_WRITTEN));
                 DataOutputStream freqStream = new DataOutputStream(new FileOutputStream(FREQ_PATH + "/part" + NUM_FILE_WRITTEN))){
                int i = 0;
                for(String key : postings.keySet()) {
                    List<Posting> postingLists = postings.get(key);
                    lexicon.updateDocIdOffset(key,i*8);
                    lexicon.updateFrequencyOffset(key,i*4);
                    for (Posting posting : postingLists) {
                        docStream.writeLong(posting.docid);
                        freqStream.writeInt(posting.frequency);
                        i++;
                    }
                    lexicon.updatedocIdSize(key, postingLists.size()*8);
                    lexicon.updatefrequencySize(key, postingLists.size()*4);
                }
                NUM_FILE_WRITTEN++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    // used for testing purpose
    public List<Posting> readFromDisk() {        // TODO handle number of num_fil_written
        List<Posting> res = new ArrayList<>();
        try (DataInputStream docStream = new DataInputStream(new FileInputStream(DOC_ID_PATH + "/part"+NUM_FILE_WRITTEN));
             DataInputStream freqStream = new DataInputStream(new FileInputStream(FREQ_PATH + "/part" + NUM_FILE_WRITTEN))) {
            while (true) {
                try{
                    long docId = docStream.readLong();
                    int frq = freqStream.readInt();
                    res.add(new Posting(docId,frq));
                }catch (EOFException eof){
                    break;
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    class Posting{
        long docid;
        int frequency;

        public Posting(long docid, int frequency) {
            this.docid = docid;
            this.frequency = frequency;
        }
    }
}
