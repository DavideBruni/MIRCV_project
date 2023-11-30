package unipi.aide.mircv.model;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.DocIdNotFoundException;
import unipi.aide.mircv.exceptions.DocumentNotFoundException;
import unipi.aide.mircv.log.CustomLogger;
import unipi.aide.mircv.queryProcessor.Scorer;

import java.io.*;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class PostingList {

    private static final String TEMP_DOC_ID_DIR ="/invertedIndex/temp/docIds";
    private static final String TEMP_FREQ_DIR ="/invertedIndex/temp/frequencies";
    private static final int POSTING_SIZE_THRESHOLD = 2048;      //2KB

    List<Posting> postingList;
    // score
    String term;
    int currentDocIdIndex;
    boolean inMemory;
    int numBlockRead;
    double idf = -1;

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

    public PostingList(String term, boolean inMemory){
        postingList = new ArrayList<>();
        currentDocIdIndex = -1;
        this.inMemory = inMemory;
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
    public PostingList readFromDisk(String token, int partition, int docIdOffset, int frequencyOffset, int numberOfPosting, boolean compressed) throws FileNotFoundException {
        PostingList res = null;
        try (FileInputStream docStream = new FileInputStream(Configuration.getRootDirectory()+TEMP_DOC_ID_DIR + "/part"+partition+".dat");
             FileInputStream freqStream = new FileInputStream(Configuration.getRootDirectory()+TEMP_FREQ_DIR + "/part" + partition+".dat")){
            res = new PostingList(token);
            if (!compressed) {
                DataInputStream dis_docStream = new DataInputStream(docStream);
                DataInputStream dis_freqStream = new DataInputStream(freqStream);
                dis_docStream.skipBytes(docIdOffset);
                dis_freqStream.skipBytes(frequencyOffset);
                for (int i = 0; i <numberOfPosting; i++) {
                    try {
                        // currently reading from two different streams (files)
                        int docId = dis_docStream.readInt();
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
                List<Integer> docIds = EliasFano.decompress(docStream, docIdOffset);
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
     * If the PostingList is not stored in memory or if an IOException is thrown from {@link #next()}, it returns {@link Integer#MAX_VALUE}.
     * Otherwise, it returns the document ID at the current index in the PostingList.
     *
     * @return The current document ID from the PostingList.
     */
    public int docId() {
        if(currentDocIdIndex == -1){        //this postingList never used
            try {
                next();
            } catch (IOException e) {
                return Integer.MAX_VALUE;
            }
        }
        if(!inMemory){                      // if it's not stored in memory, it means that I read all the blocks
            return Integer.MAX_VALUE;
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
            Posting posting = postingList.get(currentDocIdIndex);
            if(idf < 0){
                idf = Lexicon.getEntryValue(term,LexiconEntry::getIdf);
            }
            return Scorer.BM25_singleTermDocumentScore(posting.frequency,posting.docid,idf);
        } catch (DocumentNotFoundException | ArithmeticException | IndexOutOfBoundsException e ) {
            return 0;
        }
    }

    /**
     * Advances to the next document in the PostingList, updating the internal state.
     */
    public void next() throws IOException {
        /*
        * if PostingList is not in RAM and I don't already read all the blocks
        *  or if it's in memory currentDocIdINdex is greater than the postinglist size and I don't already read all the blocks
        *
        * If it's written in this way to exploit how the condition are interpreted
        * */
        Integer numBlock = Lexicon.getEntryValue(term,LexiconEntry::getNumBlocks);
        if (null == numBlock){
            inMemory = false;
        }else if((!inMemory &&  numBlock != numBlockRead) ||
                (inMemory && ++currentDocIdIndex > postingList.size() && numBlock  != numBlockRead)) {
            // qua ho considerato solo il caso in cui le posting list è divisa
            // try to read
            int docIdOffset;
            int frequencyOffset;
            if(numBlock == 1){    //check if it's divided in blocks or not
                LexiconEntry lexiconEntry = Lexicon.getEntry(term,false);
                docIdOffset = lexiconEntry.getDocIdOffset();
                frequencyOffset = lexiconEntry.getFrequencyOffset();
                numBlockRead++;
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
            inMemory = true;
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
            int postingNumber = Lexicon.getEntryValue(term,LexiconEntry::getPostingNumber);
            if (!Configuration.isCOMPRESSED()) {
                DataInputStream dis_docStream = new DataInputStream(docStream);
                DataInputStream dis_freqStream = new DataInputStream(freqStream);
                dis_docStream.skipBytes(docIdOffset);
                dis_freqStream.skipBytes(frequencyOffset);
                for (int i = 0; i < postingNumber; i++) {
                    try {
                        int docId = dis_docStream.readInt();
                        int frq = dis_freqStream.readInt();
                        res.add(new Posting(docId,frq));
                    } catch (EOFException eof) {
                        break;
                    }
                }
                dis_docStream.close();
                dis_freqStream.close();
            }else{
                List<Integer> docIds = EliasFano.decompress(docStream, docIdOffset);
                List<Integer> frequency = UnaryCompressor.readFrequencies(freqStream, frequencyOffset,postingNumber);
                int len = docIds.size();
                for(int i = 0; i<len; i++){
                    res.add(new Posting(docIds.get(i),frequency.get(i)));
                }
            }

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
    public void nextGEQ(int docId){
        Integer numBlocks = Lexicon.getEntryValue(term,LexiconEntry::getNumBlocks);

        if(inMemory && postingList.get(postingList.size() - 1).docid >= docId){
            for(int i = currentDocIdIndex; i< postingList.size(); i++){
                if(postingList.get(i).docid >= docId){
                    currentDocIdIndex = i;
                    return;
                }
            }
        }else if(numBlocks != null && ((!inMemory && numBlocks != numBlockRead) ||
                (inMemory && ++currentDocIdIndex > postingList.size() && numBlocks != numBlockRead))) {
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
                    LexiconEntry lexiconEntry = Lexicon.getEntry(term,false);
                    docIdOffset = lexiconEntry.getDocIdOffset();
                    frequencyOffset = lexiconEntry.getFrequencyOffset();
                    numBlockRead++;
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
                int len = postingList.size();
                for(int i = 0; i< len; i++){
                    if(postingList.get(i).docid >= docId){
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




    /**
     * Writes the posting lists to the specified output streams without compression.
     *
     * @param docIdStream     The output stream for document IDs.
     * @param frequencyStream The output stream for frequencies.
     * @param offset          The initial offset for output streams.
     * @param is_merged        Indicates whether the posting lists have been merged for potential skip pointers.
     * @return The final offset after writing the uncompressed data, representing the number of postings written.
     */
    public int writeToDiskNotCompressed(DataOutputStream docIdStream, DataOutputStream frequencyStream, int offset, boolean is_merged, LexiconEntry lexiconEntry) throws IOException {
        // check posting list dimension (if is the final one) to eventually create skipping-pointers
        if (is_merged && (postingList.size() * 8 > POSTING_SIZE_THRESHOLD)){
            //customLogger.info("Generating skipping pointers");
            addSkipPointers(lexiconEntry);       // we can create them directly, we have fixed dimension for docIds and frequencies
            Lexicon.setEntry(term,lexiconEntry);
        }
        for (Posting posting : postingList) {              //for each posting in postingList
            docIdStream.writeInt(posting.docid);           // writing docId
            frequencyStream.writeInt(posting.frequency);    // writing frequency
            offset++;
        }
        if(is_merged)
            Scorer.BM25_termUpperBound(postingList,lexiconEntry);
        lexiconEntry.setDocIdOffset(offset * Integer.BYTES);
        lexiconEntry.setFrequencyOffset(offset * Integer.BYTES);
        lexiconEntry.setPostingNumber(postingList.size());
        //number of posting (postiList.size()) is used for retrieve not compressed posting lists from disk

        return offset;      // is not an offset, but the number of posting lists written
    }


    /**
     * Adds skip pointers to the Lexicon entry for the given token based on the posting list.
     *
     * @param lexiconEntry  The lexicon entry associated with the token.
     */
    private void addSkipPointers(LexiconEntry lexiconEntry) {
        int blockSize = (int) Math.round(Math.sqrt(postingList.size()));
        int i = 0;
        int numBlocks = 0;
        List<SkipPointer> skippingPointers = new ArrayList<>();
        int len = postingList.size();
        for(int j = 0; j<len;j++){
            if (++i == blockSize || j == postingList.size() - 1){        // last block could be smaller
                int docIdsOffset = i * numBlocks * Integer.BYTES;
                int frequencyOffset = i * numBlocks * Integer.BYTES;
                skippingPointers.add(new SkipPointer(postingList.get(j).docid,docIdsOffset,frequencyOffset,i));
                i = 0;
                numBlocks++;
            }
        }
        if(skippingPointers.size()>1)
            numBlocks = SkipPointer.write(skippingPointers, lexiconEntry);

        lexiconEntry.setNumBlocks(numBlocks);
    }



    /**
     * Writes compressed posting lists using Elias-Fano compression for docIds and Unary compression for frequencies.
     * The method handle skipping pointers if the dimension of the posting list is above a threshold @see POSTING_SIZE_THRESHOLD
     *
     * @param docStream   The output stream for document IDs.
     * @param freqStream  The output stream for frequencies.
     * @param docOffset   The initial offset for document IDs in the output stream.
     * @param freqOffset  The initial offset for frequencies in the output stream.
     * @param is_merged   If not merged, skipping pointers are not enabled
     * @return An array of two integers representing the final offsets after writing the compressed data.
     *         The first element is the offset for document IDs, and the second element is the offset for frequencies.
     */

    public int[] writeToDiskCompressed(FileOutputStream docStream, FileOutputStream freqStream, int docOffset, int freqOffset, boolean is_merged, LexiconEntry lexiconEntry) throws IOException {
        int[] offsets = new int[]{docOffset,freqOffset};

        List<List<Posting>> postingListsToCompress = new ArrayList<>();

        int U = postingList.get(postingList.size() - 1).docid;       // greatest int to be stored
        int n = postingList.size();                                   // how many posting must be stored

        // Need Skipping Pointer
        List<SkipPointer> skipPointers = new ArrayList<>();

        // check if we are writing the final posting list for the token (is merged)
        // check if the size of compressed docIds will be above the POSTING_SIZE_TRESHOLD
        if(is_merged && ((n*Math.ceil(Math.log(U/(double)n) / Math.log(2.0)) +2*n)/8 > POSTING_SIZE_THRESHOLD)){
            //customLogger.info("Initialize skipping pointers");
            // initialize skipping pointers using heuristically optimal block size
            int blockSize = (int) Math.round(Math.sqrt(n));
            initializeSkipPointers(blockSize,postingListsToCompress,skipPointers,postingList);
        }else{
            // create anyway a list of list in order to use a unique method to write and
            // compressed posting list both if it's divided in blocks or not
            postingListsToCompress.add(postingList);
        }
        if(is_merged)
            Scorer.BM25_termUpperBound(postingList,lexiconEntry);
        lexiconEntry.setFrequencyOffset(offsets[1]);
        lexiconEntry.setDocIdOffset(offsets[0]);
        offsets = compressAndWritePostingList(postingListsToCompress,skipPointers,docStream,freqStream, offsets[0], offsets[1]);

        if(skipPointers.size() > 1) { // check if we have skipping pointers
            int numBlocks = SkipPointer.write(skipPointers, lexiconEntry);  // write them on disk
            lexiconEntry.setNumBlocks(numBlocks);           // update lexicon entry
        }

        //customLogger.info("...posting list stored");
        return offsets;

    }

    /**
     *
     * @param postingListsToCompress A list of list of postings to be compressed and written.
     * @param skipPointers           The list of skip pointers for skip compression. May be empty if no skip pointers are used.
     * @param docStream              The output stream for document IDs.
     * @param freqStream             The output stream for frequencies.
     * @param docOffset              The initial offset for writing document IDs in the output stream.
     * @param freqOffset             The initial offset for writing frequencies in the output stream.
     * @return An array of two integers representing the final offsets after writing the compressed data.
     *         The first element is the offset for document IDs, and the second element is the offset for frequencies.
     */
    private int[] compressAndWritePostingList(List<List<Posting>> postingListsToCompress, List<SkipPointer> skipPointers, FileOutputStream docStream, FileOutputStream freqStream, int docOffset, int freqOffset) throws IOException {
        // if posting list is not divided in blocks, this for have a single iteration
        for(List<Posting> postingList : postingListsToCompress){
            //customLogger.info("Compressing document ids");
            EliasFanoCompressedList eliasFanoCompressedDocIdList = EliasFano.compress(postingList);
            //customLogger.info("Compressing term frequencies");
            List<BitSet> unaryCompressedFrequencyList = UnaryCompressor.compress(postingList);

            //customLogger.info("Writing compressed document ids");
            if(skipPointers.size()>0)
                skipPointers.get(postingListsToCompress.indexOf(postingList)).setDocIdOffset(docOffset);
            docOffset += eliasFanoCompressedDocIdList.writeToDisk(docStream);

            //customLogger.info("Writing compressed term frequencies");
            if(skipPointers.size()>0)
                skipPointers.get(postingListsToCompress.indexOf(postingList)).setFrequencyOffset(freqOffset);
            freqOffset = UnaryCompressor.writeToDisk(unaryCompressedFrequencyList,freqOffset, freqStream);

            // update skipPointer parameters
            if(skipPointers.size()>0) {
                SkipPointer skipPointer = skipPointers.get(postingListsToCompress.indexOf(postingList));
                skipPointer.setMaxDocId(postingList.get(postingList.size()-1).docid);
                skipPointer.setNumberOfDocId(postingList.size());
            }
        }

        return new int[]{docOffset, freqOffset};
    }

    /**
     * Initializes skip pointers by dividing the posting list into blocks.
     *
     * @param blockSize             The number of postings in each block.
     * @param postingListsToCompress The list to store sublists of postings, each representing a block.
     * @param skipPointers          The list to store skip pointers corresponding to each block.
     * @param postingLists          The original posting list to be compressed.
     */
    private void initializeSkipPointers(int blockSize, List<List<Posting>> postingListsToCompress, List<SkipPointer> skipPointers, List<Posting> postingLists) {
        int i=0;
        List<Posting> tmp = new ArrayList<>();

        while(true){
            try {
                tmp.add(postingLists.get(i++));             // take one posting list at time and accumulate them
                if (i % blockSize == 0){                    //Each block contains blockSize postings
                    postingListsToCompress.add(tmp);        // populate the list of posting list
                    skipPointers.add(new SkipPointer());
                    tmp.clear();
                }
            }catch (IndexOutOfBoundsException ex){
                if(!tmp.isEmpty()) {     //last block could be smaller, so the previous block could throw this exception
                    postingListsToCompress.add(tmp);
                    skipPointers.add(new SkipPointer());
                }
                break;
            }
        }
    }




    @Override
    public String toString() {
        return "PostingList{" +
                "postingList=" + postingList +
                ", term='" + term + '\'' +
                ", currentDocIdIndex=" + currentDocIdIndex +
                ", inMemory=" + inMemory +
                ", numBlockRead=" + numBlockRead +
                '}';
    }
}
