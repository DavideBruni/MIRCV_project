package unipi.aide.mircv.model;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.DocIdNotFoundException;
import unipi.aide.mircv.exceptions.DocumentNotFoundException;
import unipi.aide.mircv.log.CustomLogger;
import unipi.aide.mircv.queryProcessor.Scorer;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PostingList {

    private static final String TEMP_DOC_ID_DIR ="data/invertedIndex/temp/docIds";
    private static final String TEMP_FREQ_DIR ="data/invertedIndex/temp/frequencies";

    List<Posting> postingList;
    // score
    String term;
    int currentDocIdIndex;
    boolean inMemory;
    int numBlockRead;

    // constructors

    public PostingList(List<Posting> postingList,String term) {
        this.term = term;
        this.postingList = postingList;
        currentDocIdIndex = -1;
        inMemory = true;
    }

    public PostingList() {
        postingList = new ArrayList<>();
        currentDocIdIndex = -1;
        inMemory = true;
    }

    public PostingList(String term){
        postingList = new ArrayList<>();
        currentDocIdIndex = -1;
        inMemory = true;
        this.term = term;

    }


    /**
     * Reads a PostingList from disk for a specified token and partition.
     *
     * @param token            The token for which to read the PostingList.
     * @param partition        The partition index from which to read the PostingList.
     * @param docIdOffset      The offset within the document ID file from where to start reading.
     * @param frequencyOffset  The offset within the frequency file from where to start reading.
     * @param numberOfPosting  The number of postings to read.
     * @param compressed       A boolean indicating whether the data is compressed.
     * @return A PostingList object.
     */
    public PostingList readFromDisk(String token, int partition, int docIdOffset, int frequencyOffset, int numberOfPosting, boolean compressed) {
        PostingList res = null;
        try (FileInputStream docStream = new FileInputStream(TEMP_DOC_ID_DIR + "/part"+partition+".dat");
             FileInputStream freqStream = new FileInputStream(TEMP_FREQ_DIR + "/part" + partition+".dat")){
            res = new PostingList(token);
            if (!compressed) {
                DataInputStream dis_docStream = new DataInputStream(docStream);
                DataInputStream dis_freqStream = new DataInputStream(freqStream);
                dis_docStream.skipBytes(docIdOffset);
                dis_freqStream.skipBytes(frequencyOffset);
                for (int i = 0; i < numberOfPosting; i++) {
                    try {
                        // currently reading from two different streams (files)
                        long docId = dis_docStream.readLong();
                        int frq = dis_freqStream.readInt();
                        res.add(new Posting(docId,frq));        //add the Posting to the postingList
                    } catch (EOFException eof) {
                        break;                                  //there are no more postings
                    }
                }
                dis_docStream.close();
                dis_freqStream.close();
            }else{
                // reading docIds first, then frequencies and storing them in separate lists (however they are linked by the index in the list)
                List<Long> docIds = EliasFano.decompress(docStream, docIdOffset);
                List<Integer> frequency = UnaryCompressor.readFrequencies(freqStream,frequencyOffset,docIds.size());

                // now for each pair docId - frequency I create a Posting and add it to the postinglist
                for(int i = 0; i<docIds.size(); i++){
                    res.add(new Posting(docIds.get(i),frequency.get(i)));
                }
            }

        } catch (IOException e) {
            CustomLogger.error("Error while retrieving posting list: "+e.getMessage());
        }
        return res;
    }

    /**
     * Retrieves the current document ID from the PostingList.
     * If the PostingList has never been used (initialized), this method calls {@link #next()} to set the initial state.
     * If the PostingList is not stored in memory or if an IOException is thrown from {@link #next()}, it returns {@link Long#MAX_VALUE}.
     * Otherwise, it returns the document ID at the current index in the PostingList.
     *
     * @return The current document ID from the PostingList.
     */
    public long docId() {
        if(currentDocIdIndex == -1){        //this postingList never used
            try {
                next();
            } catch (IOException e) {
                return Long.MAX_VALUE;
            }
        }
        if(!inMemory){                      // if it's not stored in memory, it means that I read all the blocks
            return Long.MAX_VALUE;
        }
        return postingList.get(currentDocIdIndex).docid;
    }

    public void add(Posting posting) {
        postingList.add(posting);
    }

    public List<Posting> getPostingList() {
        return postingList;
    }

    public double score(){
        try {
            return Scorer.BM25_singleTermDocumentScore(postingList.get(currentDocIdIndex).frequency,postingList.get(currentDocIdIndex).docid, Lexicon.getEntryValue(term,LexiconEntry::getIdf) );
        } catch (DocumentNotFoundException | ArithmeticException e ) {
            return 0;
        }
    }

    /**
     * Advances to the next document in the PostingList, updating the internal state.
     */
    public void next() throws IOException {
        /*
        * if PostingList is not in RAM and I don't already read all the blocks
        *  or if it's in memory currentDocIdINdex is greater than the postinglist size and I dont' already read all the blocks
        *
        * If it's written in this way to exploit how the condition are interpreted
        * */
        if((!inMemory && Lexicon.getEntryValue(term,LexiconEntry::getNumBlocks)  != numBlockRead) ||
                (inMemory && ++currentDocIdIndex >= postingList.size() && Lexicon.getEntryValue(term,LexiconEntry::getNumBlocks)  != numBlockRead)) {
            // qua ho considerato solo il caso in cui le posting list è divisa
            // try to read
            int docIdOffset = 0;
            int frequencyOffset = 0;
            if(Lexicon.getEntryValue(term,LexiconEntry::getNumBlocks) == 1){    //check if it's divided in blocks or not
                LexiconEntry lexiconEntry = Lexicon.getEntry(term);
                docIdOffset = lexiconEntry.getDocIdOffset();
                frequencyOffset = lexiconEntry.getFrequencyOffset();
            }else{
                SkipPointer skipPointer;
                try {
                    skipPointer = SkipPointer.readFromDisk(Lexicon.getEntryValue(term,LexiconEntry::getSkipPointerOffset) ,numBlockRead);
                } catch (IOException e) {
                    throw new IOException(e);
                }
                numBlockRead++;
                docIdOffset = skipPointer.getDocIdsOffset();
                frequencyOffset = skipPointer.getFrequencyOffset();
            }
            postingList = readFromDisk(docIdOffset,frequencyOffset);
            currentDocIdIndex = 0;              // reset docIdIndex to the start
        }else if(! (currentDocIdIndex < postingList.size())){
            inMemory = false;
        }
    }


    // similar to the other method readFromDisk, but this method is used only internally at the class, moreover is not for partition
    // and errors are not handled inside the method
    private List<Posting> readFromDisk(int docIdOffset, int frequencyOffset) throws IOException {
        List<Posting> res = new ArrayList<>();
        try (FileInputStream docStream = new FileInputStream(Configuration.getDocumentIdsPath());
             FileInputStream freqStream = new FileInputStream(Configuration.getFrequencyPath())){
            if (!Configuration.isCOMPRESSED()) {
                DataInputStream dis_docStream = new DataInputStream(docStream);
                DataInputStream dis_freqStream = new DataInputStream(freqStream);
                dis_docStream.skipBytes(docIdOffset);
                dis_freqStream.skipBytes(frequencyOffset);
                for (int i = 0; i < Lexicon.getEntryValue(term,LexiconEntry::getPostingNumber); i++) {
                    try {
                        long docId = dis_docStream.readLong();
                        int frq = dis_freqStream.readInt();
                        res.add(new Posting(docId,frq));
                    } catch (EOFException eof) {
                        break;
                    }
                }
                dis_docStream.close();
                dis_freqStream.close();
            }else{
                List<Long> docIds = EliasFano.decompress(docStream, docIdOffset);
                List<Integer> frequency = UnaryCompressor.readFrequencies(freqStream, frequencyOffset,Lexicon.getEntryValue(term,LexiconEntry::getPostingNumber));
                for(int i = 0; i<docIds.size(); i++){
                    res.add(new Posting(docIds.get(i),frequency.get(i)));
                }
            }

        } catch (IOException e) {
            throw e;
        }
        return res;
    }


    /**
     * Advances the current document index to the next document ID greater than or equal to the specified docId.
     * This method efficiently navigates through the PostingList, whether it's in memory or stored on disk in blocks.
     * If the desired docId is found in the current in-memory PostingList, the currentDocIdIndex is updated accordingly.
     * If the PostingList is not in memory or the docId is not found, it attempts to read the necessary blocks from disk.
     *
     * @param docId The target document ID to seek in the PostingList.
     */
    public void nextGEQ(long docId){
        int numBlocks = Lexicon.getEntryValue(term,LexiconEntry::getNumBlocks);

        if(inMemory && postingList.get(postingList.size() - 1).docid > docId){
            for(int i = 0; i< postingList.size(); i++){
                if(docId == postingList.get(i).docid){
                    currentDocIdIndex = i;
                    return;
                }
            }
        }else if((!inMemory && numBlocks != numBlockRead) ||
                (inMemory && ++currentDocIdIndex >= postingList.size() && numBlocks != numBlockRead)) {
            /*
             * if PostingList is not in RAM and I don't already read all the blocks
             *  or if it's in memory currentDocIdINdex is greater than the postinglist size and I don't already read all the blocks
             *
             * If it's written in this way to exploit how the condition are interpreted
             * */
            try {
                int docIdOffset;
                int frequencyOffset;
                if(numBlocks == 1){    //check if it's divided in blocks or not
                    LexiconEntry lexiconEntry = Lexicon.getEntry(term);
                    docIdOffset = lexiconEntry.getDocIdOffset();
                    frequencyOffset = lexiconEntry.getFrequencyOffset();
                }else {
                    SkipPointer skipPointer;
                    while (true) {
                        skipPointer = SkipPointer.readFromDisk(numBlocks, numBlockRead);
                        numBlockRead++;
                        if (skipPointer.getMaxDocId() >= docId)
                            break;
                        if (numBlocks == numBlockRead)
                            throw new DocIdNotFoundException();
                    }
                    docIdOffset = skipPointer.getDocIdsOffset();
                    frequencyOffset = skipPointer.getFrequencyOffset();
                }
                postingList = readFromDisk(docIdOffset, frequencyOffset);
                for(int i = 0; i< postingList.size(); i++){
                    if(docId == postingList.get(i).docid){
                        currentDocIdIndex = i;
                        return;
                    }
                }
            }catch(DocIdNotFoundException | IOException d) {
                inMemory = false;
            }
        }else{
            inMemory = false;
        }
    }

    public double getTermUpperBound() {
        return Lexicon.getEntryValue(term,LexiconEntry::getTermUpperBound);
    }

    public String getToken() { return term; }
}
