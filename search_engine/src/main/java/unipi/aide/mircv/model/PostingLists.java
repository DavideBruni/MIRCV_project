package unipi.aide.mircv.model;

import unipi.aide.mircv.helpers.FileHelper;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class PostingLists {

    Map<String, PostingList> postings;
    private static final String TEMP_DOC_ID_DIR ="data/invertedIndex/temp/docIds";
    private static final String TEMP_FREQ_DIR ="data/invertedIndex/temp/frequencies";
    private static int NUM_FILE_WRITTEN = 0;
    private static final int POSTING_SIZE_THRESHOLD = 2048;      //2KB

    public PostingLists() {
        this.postings = new HashMap<>();
    }

    private PostingLists(List<Long> docIds, List<Integer> frequencies, String token) {
        this.postings = new HashMap<>();
        List<Posting> postingList = new ArrayList<>();
        for(int i = 0; i<docIds.size();i++){
            postingList.add(new Posting(docIds.get(i), frequencies.get(i)));
        }
        postings.put(token,new PostingList(postingList,token));
    }


    public PostingLists readFromDisk(String token, int partition, int docIdOffset, int frequencyOffset, int numberOfPosting, boolean compressed) {
        PostingLists res;
        try (FileInputStream docStream = new FileInputStream(TEMP_DOC_ID_DIR + "/part"+partition+".dat");
            FileInputStream freqStream = new FileInputStream(TEMP_FREQ_DIR + "/part" + partition+".dat")){
            if (!compressed) {
                res = new PostingLists();
                DataInputStream dis_docStream = new DataInputStream(docStream);
                DataInputStream dis_freqStream = new DataInputStream(freqStream);
                dis_docStream.skipBytes(docIdOffset);
                dis_freqStream.skipBytes(frequencyOffset);
                for (int i = 0; i < numberOfPosting; i++) {
                    try {
                        long docId = dis_docStream.readLong();
                        int frq = dis_freqStream.readInt();
                        res.add(docId, token, frq);
                    } catch (EOFException eof) {
                        break;
                    }
                }
                dis_docStream.close();
                dis_freqStream.close();
            }else{
                List<Long> docIds = EliasFano.decompress(docStream, docIdOffset);
                List<Integer> frequency = UnaryCompressor.readFrequencies(freqStream, frequencyOffset,numberOfPosting);
                res = new PostingLists(docIds, frequency,token);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    public void add(long docId, String token, int frequency) {
        if (!postings.containsKey(token)){
            postings.put(token, new PostingList());
        }
        postings.get(token).add(new Posting(docId, frequency));
    }

    public void add(List<PostingLists> postingLists, String token) {
        List<Posting> postings_merged = new ArrayList<>();
        for(PostingLists postingList : postingLists){        // per lo stesso token devo fare il merge in unica p.l
            postings_merged.addAll(postingList.postings.get(token).getPostingList());
        }
        postings.put(token, new PostingList(postings_merged,token));               //ottenuta la pl, la associo al token
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

        try (FileOutputStream docStream = new FileOutputStream(TEMP_DOC_ID_DIR + "/part" + NUM_FILE_WRITTEN + ".dat");
             FileOutputStream freqStream = new FileOutputStream(TEMP_FREQ_DIR + "/part" + NUM_FILE_WRITTEN + ".dat")){
            if (!compressed){
                try(DataOutputStream docStream_dos = new DataOutputStream(docStream);
                    DataOutputStream freqStream_dos = new DataOutputStream(freqStream)) {
                        writeToDiskNotCompressed(lexicon, docStream_dos, freqStream_dos, 0, false);
                }
            }else{
                writeToDiskCompressed(lexicon, docStream, freqStream,0, 0, false);
            }
            NUM_FILE_WRITTEN++;
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public int[] writeToDiskCompressed(Lexicon lexicon, FileOutputStream docStream, FileOutputStream freqStream, int docOffset, int freqOffset, boolean is_merged) throws IOException {
        int offsets [] = new int[]{docOffset,freqOffset};

        for(String token: postings.keySet()){
            List<List<Posting>> postingListsToCompress = new ArrayList<>();
            List<Posting> postingLists = postings.get(token).getPostingList();

            long U = postingLists.get(postingLists.size() - 1).docid;
            long n = postingLists.size();

            // Need Skipping Pointer
            LexiconEntry lexiconEntry = lexicon.getEntry(token);
            List<SkipPointer> skipPointers = new ArrayList<>();

            if(is_merged && ((n*Math.ceil(Math.log(U/(double)n) / Math.log(2.0)) +2*n)/8 > POSTING_SIZE_THRESHOLD)){
                int blockSize = (int) Math.round(Math.sqrt(postings.get(token).getPostingList().size()));
                initializeSkipPointers(blockSize,postingListsToCompress,skipPointers,postingLists);
            }else{
                postingListsToCompress.add(postingLists);
            }
                offsets = compressAndWritePostingList(postingListsToCompress,skipPointers,lexiconEntry,docStream,freqStream, docOffset, freqOffset);
            if(skipPointers.size() > 1) {
                int numBlocks = SkipPointer.write(skipPointers, lexiconEntry);
                lexiconEntry.setNumBlocks(numBlocks);
                lexicon.setEntry(token, lexiconEntry);
            }

            lexicon.setEntry(token,lexiconEntry);

        }
        return offsets;

    }

    private int[] compressAndWritePostingList(List<List<Posting>> postingListsToCompress, List<SkipPointer> skipPointers, LexiconEntry lexiconEntry, FileOutputStream docStream, FileOutputStream freqStream, int docOffset, int freqOffset) throws IOException {
        lexiconEntry.setDocIdOffset(docOffset);
        lexiconEntry.setFrequencyOffset(freqOffset);

        for(List<Posting> postingList : postingListsToCompress){
            EliasFanoCompressedList eliasFanoCompressedDocIdList = EliasFano.compress(postingList);
            List<BitSet> unaryCompressedFrequencyList = UnaryCompressor.compress(postingList);

            if(skipPointers.size()>0)
                skipPointers.get(postingListsToCompress.indexOf(postingList)).setDocIdOffset(docOffset);
            eliasFanoCompressedDocIdList.writeToDisk(docStream);
            docOffset = eliasFanoCompressedDocIdList.getSize();

            if(skipPointers.size()>0)
                skipPointers.get(postingListsToCompress.indexOf(postingList)).setFrequencyOffset(freqOffset);
            freqOffset = UnaryCompressor.writeToDisk(unaryCompressedFrequencyList,freqOffset, freqStream);

            if(skipPointers.size()>0)
                skipPointers.get(postingListsToCompress.indexOf(postingList)).setNumberOfDocId(postingList.size());
        }

        return new int[]{docOffset, freqOffset};
    }

    private void initializeSkipPointers(int blockSize, List<List<Posting>> postingListsToCompress, List<SkipPointer> skipPointers, List<Posting> postingLists) {
        int i=0;
        List<Posting> tmp = new ArrayList<>();
        // meccanismo per prendere blockSize posting per volta
        while(true){
            try {
                tmp.add(postingLists.get(i++));
                if (i % blockSize == 0){      //ho riempito un blocco
                    postingListsToCompress.add(tmp);
                    skipPointers.add(new SkipPointer());
                    tmp.clear();
                }
            }catch (IndexOutOfBoundsException ex){
                if(!tmp.isEmpty()) {     //potrebbe essere che l'ultima non è più piccola, e quindi faccio un'iterazione in più dove il primo accesso genererà un'eccezione
                    postingListsToCompress.add(tmp);
                    skipPointers.add(new SkipPointer());
                }
                break;
            }
        }
    }

    public int writeToDiskNotCompressed(Lexicon lexicon, DataOutputStream docIdStream, DataOutputStream frequencyStream, int offset, boolean is_merged) throws IOException {
        for(String token: postings.keySet()){
            List<Posting> postingLists = postings.get(token).getPostingList();
            lexicon.updateDocIdOffset(token, offset * Long.BYTES);
            lexicon.updateFrequencyOffset(token, offset * Integer.BYTES);
            if (is_merged && postingLists.size() * (Long.BYTES + Integer.BYTES) > POSTING_SIZE_THRESHOLD){
                LexiconEntry lexiconEntry = lexicon.getEntry(token);
                addSkipPointers(token, lexiconEntry);
                lexicon.setEntry(token,lexiconEntry);
            }
            for (Posting posting : postingLists) {
                docIdStream.writeLong(posting.docid);
                frequencyStream.writeInt(posting.frequency);
                offset++;
            }
            lexicon.setNumberOfPostings(token,postingLists.size());
        }
        try {
            docIdStream.close();
            frequencyStream.close();
        }catch(IOException e){
            // do something
        }
        return offset;      // in realtà è il numero di posting list scritte

    }


    private void addSkipPointers(String token, LexiconEntry lexiconEntry) {
        // if some error, return 1
        List<Posting> token_postings = postings.get(token).getPostingList();
        int blockSize = (int) Math.round(Math.sqrt(token_postings.size()));
        int i = 0;
        int numBlocks = 0;
        List<SkipPointer> skippingPointers = new ArrayList<>();
        for(int j = 0; j<token_postings.size();j++){
            if (++i == blockSize || j == token_postings.size() - 1){        // last block could be smaller
                int docIdsOffset = i * numBlocks * Long.BYTES;
                int frequencyOffset = i * numBlocks * Integer.BYTES;
                skippingPointers.add(new SkipPointer(token_postings.get(j).docid,docIdsOffset,frequencyOffset,i));
                i = 0;
                numBlocks++;
            }
        }
        if(skippingPointers.size()>1)
            numBlocks = SkipPointer.write(skippingPointers, lexiconEntry);

        lexiconEntry.setNumBlocks(numBlocks);

    }
}
