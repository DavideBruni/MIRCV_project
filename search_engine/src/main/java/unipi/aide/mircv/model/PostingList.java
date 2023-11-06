package unipi.aide.mircv.model;

import unipi.aide.mircv.fileHelper.FileHelper;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class PostingList {

    Map<String, List<Posting>> postings;
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

    public int add(List<PostingList> postingLists, String token) {
        int posting_size = 12;
        List<Posting> postings_merged = new ArrayList<>();
        for(PostingList postingList : postingLists){        // per lo stesso token devo fare il merge in unica p.l
            postings_merged.addAll(postingList.postings.get(token));
        }
        postings.put(token, postings_merged);               //ottenuta la pl, la associo al token
        return postings_merged.size()*posting_size;
    }

    public PostingList readFromDisk(String token, int partition, int docIdOffset, int frequencyOffset, int docIdSize, int frequencySize) {
        PostingList res = new PostingList();
        try (DataInputStream docStream = new DataInputStream(new FileInputStream(DOC_ID_PATH + "/part"+partition));
             DataInputStream freqStream = new DataInputStream(new FileInputStream(FREQ_PATH + "/part" + partition))) {
            docStream.skipBytes(docIdOffset);
            freqStream.skipBytes(frequencyOffset);
            for(int i = 0; i< docIdSize/8; i++) {
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
                    i = writeToDiskNotCompressed(lexicon, docStream, freqStream, key,i);
                }
                NUM_FILE_WRITTEN++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public int writeToDiskNotCompressed(Lexicon lexicon, DataOutputStream docIdStream, DataOutputStream frequencyStream,String token, int offset) throws IOException {

        List<Posting> postingLists = postings.get(token);
        lexicon.updateDocIdOffset(token,offset*8);
        lexicon.updateFrequencyOffset(token,offset*4);
        for (Posting posting : postingLists) {
            docIdStream.writeLong(posting.docid);
            frequencyStream.writeInt(posting.frequency);
            offset++;
        }
        lexicon.updatedocIdSize(token, postingLists.size()*8);
        lexicon.updatefrequencySize(token, postingLists.size()*4);
        return offset;

    }


    public void addSkipPointers(String token, LexiconEntry lexiconEntry, boolean compressed) {
        // if some error, return 1
        List<Posting> token_postings = postings.get(token);
        int blockSize = (int) Math.round(Math.sqrt(token_postings.size()));
        int i = 0;
        int numBlocks = 0;
        List<SkipPointer> skippingPointers = new ArrayList<>();
        for(int j = 0; j<token_postings.size();j++){
            if (++i == blockSize || j == token_postings.size() - 1){        // last block could be smaller
                int docIdsOffset = 0;
                int frequencyOffset = 0;
                if (compressed){
                    docIdsOffset = i * numBlocks * 8;
                    frequencyOffset = i * numBlocks * 4;
                }
                    skippingPointers.add(new SkipPointer(token_postings.get(j).docid,docIdsOffset,frequencyOffset));
                i = 0;
                numBlocks++;
            }
        }
        if(skippingPointers.size()>1)
            numBlocks = SkipPointer.write(skippingPointers, lexiconEntry);

        lexiconEntry.setNumBlock(numBlocks);

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
