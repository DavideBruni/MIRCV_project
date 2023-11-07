package unipi.aide.mircv.model;

import unipi.aide.mircv.fileHelper.FileHelper;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class PostingList {

    Map<String, List<Posting>> postings;
    private static final String TEMP_DOC_ID_DIR ="data/invertedIndex/temp/docIds";
    private static final String TEMP_FREQ_DIR ="data/invertedIndex/temp/frequencies";
    private static int NUM_FILE_WRITTEN = 0;

    // prova di Elias-Fano
    private static List<Integer> maxDocId = null;

    public PostingList() {
        this.postings = new HashMap<>();
    }


    public PostingList readFromDisk(String token, int partition, int docIdOffset, int frequencyOffset, int docIdSize, int frequencySize) {
        PostingList res = new PostingList();
        try (DataInputStream docStream = new DataInputStream(new FileInputStream(TEMP_DOC_ID_DIR + "/part"+partition+".dat"));
             DataInputStream freqStream = new DataInputStream(new FileInputStream(TEMP_FREQ_DIR + "/part" + partition+".dat"))) {
            docStream.skipBytes(docIdOffset);
            freqStream.skipBytes(frequencyOffset);
            for(int i = 0; i< docIdSize/Long.BYTES; i++) {
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
        postings.get(token).add(new Posting(docId, frequency));
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
        FileHelper.createDir(TEMP_DOC_ID_DIR);
        FileHelper.createDir(TEMP_FREQ_DIR);
        if (!compressed){
            try (DataOutputStream docStream = new DataOutputStream(new FileOutputStream(TEMP_DOC_ID_DIR + "/part" + NUM_FILE_WRITTEN + ".dat"));
                 DataOutputStream freqStream = new DataOutputStream(new FileOutputStream(TEMP_FREQ_DIR + "/part" + NUM_FILE_WRITTEN + ".dat"))){
                int i = 0;
                for(String key : postings.keySet()) {
                    i = writeToDiskNotCompressed(lexicon, docStream, freqStream, key,i);
                }
                NUM_FILE_WRITTEN++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
              if (maxDocId == null)
                  maxDocId = new ArrayList<>();
              maxDocId.add(CollectionStatistics.getCollectionSize());

        }

    }

    public void writeToDiskCompressed(Lexicon lexicon, String token){

    }

    public int writeToDiskNotCompressed(Lexicon lexicon, DataOutputStream docIdStream, DataOutputStream frequencyStream,String token, int offset) throws IOException {
        List<Posting> postingLists = postings.get(token);
        lexicon.updateDocIdOffset(token,offset*Long.BYTES);
        lexicon.updateFrequencyOffset(token,offset* Integer.BYTES);
        for (Posting posting : postingLists) {
            docIdStream.writeLong(posting.docid);
            frequencyStream.writeInt(posting.frequency);
            offset++;
        }
        lexicon.updatedocIdSize(token, postingLists.size()*Long.BYTES);
        lexicon.updatefrequencySize(token, postingLists.size()*Integer.BYTES);
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

        lexiconEntry.setNumBlocks(numBlocks);

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
