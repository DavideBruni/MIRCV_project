package unipi.aide.mircv.model;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.DocIdNotFoundException;
import unipi.aide.mircv.exceptions.DocumentNotFoundException;
import unipi.aide.mircv.log.CustomLogger;
import unipi.aide.mircv.queryProcessor.Scorer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class PostingList implements Serializable{

    private static final String TEMP_DOC_ID_DIR ="/invertedIndex/temp/docIds";
    private static final String TEMP_FREQ_DIR ="/invertedIndex/temp/frequencies";
    private static final int POSTING_SIZE_THRESHOLD = 2048;      //2KB
    private static final String SCORE_STANDARD = Configuration.getScoreStandard();
    List<Integer> docIds;
    List<Integer> frequencies;
    // score
    String term;
    int currentDocIdIndex;
    boolean inMemory;
    int numBlockRead;
    double idf = -1;

    // constructors

    /*public PostingList(List<Posting> postingList,String term) {
        this.term = term;
        this.postingList = postingList;
        currentDocIdIndex = -1;
        inMemory = true;
    }
*/
    public PostingList() {
        docIds = new ArrayList<>();
        frequencies = new ArrayList<>();
        currentDocIdIndex = -1;
        inMemory = true;
    }
/*
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
    }*/

    public PostingList(int[] docIdsArray, int[] freqArray, String token) {
        docIds = new ArrayList<>();
        for(int docId : docIdsArray)
            docIds.add(docId);
        frequencies = new ArrayList<>();
        for(int freq : freqArray)
            frequencies.add(freq);
        term = token;
    }

    public PostingList(List<Integer> docIds, List<Integer> frequencies, String term) {
        this.docIds = docIds;
        this.frequencies = frequencies;
        this.term = term;
    }

    /*public int getSize(){
        return postingList.size();
    }*/

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
    public PostingList readFromDisk(String token, int partition, int docIdOffset, int frequencyOffset, int numberOfPosting, int maxDocId, boolean compressed) throws FileNotFoundException {
        PostingList res = null;
        try(FileChannel docStream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getRootDirectory()+TEMP_DOC_ID_DIR + "/part"+partition+".dat"),
                                                                        StandardOpenOption.READ);
            FileChannel freqStream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getRootDirectory()+TEMP_FREQ_DIR + "/part" + partition+".dat"),
                                                                        StandardOpenOption.READ))
        {
            docStream.position(docIdOffset);
            freqStream.position(frequencyOffset);
            if (!compressed) {
                ByteBuffer bufferDocIds = ByteBuffer.allocateDirect(4*numberOfPosting);
                ByteBuffer bufferFrequencies = ByteBuffer.allocateDirect(4*numberOfPosting);
                docStream.read(bufferDocIds);
                freqStream.read(bufferFrequencies);
                bufferDocIds.flip();
                bufferFrequencies.flip();
                int [] docIdsArray = new int[numberOfPosting];
                bufferDocIds.asIntBuffer().get(docIdsArray);
                int [] freqArray = new int[numberOfPosting];
                bufferFrequencies.asIntBuffer().get(freqArray);

                res = new PostingList(docIdsArray,freqArray,token);

            }else{
                // reading docIds first, then frequencies and storing them in separate lists (however they are linked by the index in the list)
                ByteBuffer bufferDocIds = ByteBuffer.allocateDirect(EliasFano.getCompressedSize(maxDocId,numberOfPosting));
                int len = docStream.read(bufferDocIds);
                bufferDocIds.flip();
                byte [] buffer = new byte[len];
                bufferDocIds.get(buffer);
                List<Integer> documentIds = EliasFano.decompress(buffer, numberOfPosting,maxDocId);
                List<Integer> freqs = UnaryCompressor.readFrequencies(freqStream,frequencyOffset,numberOfPosting);
                res = new PostingList(documentIds,freqs,token);

            }

        } catch (IOException e) {
            CustomLogger.error("Error while retrieving posting list: "+e.getMessage());
        }
        return res;
    }

    /**
     * Retrieves the current document ID from the PostingList.
     * If the PostingList has never been used (initialized), this method calls {@link #//next()} to set the initial state.
     * If the PostingList is not stored in memory or if an IOException is thrown from {@link #//next()}, it returns {@link Integer#MAX_VALUE}.
     * Otherwise, it returns the document ID at the current index in the PostingList.
     *
     * @return The current document ID from the PostingList.
     */
    /*
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
*/
    public void add(int docId, int frequency) {
        docIds.add(docId);
        frequencies.add(frequency);
    }
/*
    public List<Posting> getPostingList() {
        return postingList;
    }

    public double score(){
        try {
            Posting posting = postingList.get(currentDocIdIndex);
            if(idf < 0){
                idf = Lexicon.getEntryValue(term,LexiconEntry::getIdf);
            }
            if(SCORE_STANDARD.equals("BM25"))
                return Scorer.BM25_singleTermDocumentScore(posting.frequency,posting.docid,idf);
            else {
                return Scorer.TFIDF_singleTermDocumentScore(posting.frequency,idf);
            }
        } catch (DocumentNotFoundException | ArithmeticException | IndexOutOfBoundsException e ) {
            return 0;
        }
    }
*/
    /**
     * Advances to the next document in the PostingList, updating the internal state.
     */
    /*
    public void next() throws IOException {
        /*
        * if PostingList is not in RAM and I don't already read all the blocks
        *  or if it's in memory currentDocIdINdex is greater than the postinglist size and I don't already read all the blocks
        *
        * If it's written in this way to exploit how the condition are interpreted
        * */
            /*
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
                LexiconEntry lexiconEntry = Lexicon.getEntry(term);
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
                List<Integer> docIds = new ArrayList<>();//EliasFano.decompress(docStream, docIdOffset);
                List<Integer> frequency = UnaryCompressor.readFrequencies(freqStream, frequencyOffset,postingNumber);
                int len = docIds.size();
                for(int i = 0; i<len; i++){
                    res.add(new Posting(docIds.get(i),frequency.get(i)));
                }
            }

        }
        return res;
    }

             */


    /*
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
    /*
            try {
                int docIdOffset;
                int frequencyOffset;
                if(numBlocks == 1){    //check if it's divided in blocks or not
                    LexiconEntry lexiconEntry = Lexicon.getEntry(term);
                    docIdOffset = lexiconEntry.getDocIdOffset();
                    frequencyOffset = lexiconEntry.getFrequencyOffset();
                    numBlockRead++;
                }else {
                    SkipPointer skipPointer;
                    while (true) {
                        skipPointer = SkipPointer.readFromDisk(Lexicon.getEntryValue(term,LexiconEntry::getSkipPointerOffset), numBlockRead);
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
        return Lexicon.getEntryValue(term,LexiconEntry::getBM25_termUpperBound);
    }

    public String getToken() { return term; }



*/
    public int writeToDiskNotCompressed(FileChannel docIdStream, FileChannel frequencyStream, int offset) throws IOException {
        ByteBuffer idBuffer = ByteBuffer.allocateDirect(docIds.size()*Integer.BYTES);
        ByteBuffer freqBuffer = ByteBuffer.allocateDirect(docIds.size()*Integer.BYTES);
        for (int i = 0; i< docIds.size(); i++) {              //for each posting in postingList
            idBuffer.putInt(docIds.get(i));
            freqBuffer.putInt(frequencies.get(i));
            offset++;
        }
        idBuffer.flip();
        freqBuffer.flip();
        docIdStream.write(idBuffer);
        frequencyStream.write(freqBuffer);

        return offset;      // is not an offset, but the number of posting lists written
    }


    public int[] writeToDiskCompressed(FileChannel docStream, FileChannel freqStream, int docOffset, int freqOffset, LexiconEntry lexiconEntry) throws IOException {
        int[] offsets = new int[]{docOffset,freqOffset};
        lexiconEntry.setFrequencyOffset(offsets[1]);
        lexiconEntry.setDocIdOffset(offsets[0]);
        lexiconEntry.setMaxId(docIds.get(docIds.size()-1));
        offsets = compressAndWritePostingList(docStream,freqStream, offsets[0], offsets[1]);

        return offsets;

    }

    private int[] compressAndWritePostingList(FileChannel docStream, FileChannel freqStream, int docOffset, int freqOffset) throws IOException {
        int maxDocId = docIds.get(docIds.size()-1);
        int n = docIds.size();
        byte [] compressedId = new byte[EliasFano.getCompressedSize(maxDocId,n)];
        final int l = EliasFano.getL(maxDocId, n);
        long lowBitsOffset = 0;
        long highBitsOffset = EliasFano.roundUp(l * n, Byte.SIZE);
        EliasFano.compress(docIds,compressedId,l,new long[]{lowBitsOffset,highBitsOffset},0);
        docOffset += EliasFano.writeToDisk(compressedId,docStream);
        List<BitSet> unaryCompressedFrequencyList = UnaryCompressor.compress(frequencies);
        freqOffset = UnaryCompressor.writeToDisk(unaryCompressedFrequencyList,freqStream,freqOffset);
        return new int[]{docOffset, freqOffset};
    }



    public List<Integer> getDocIds() {
        return docIds;
    }

    public List<Integer> getFrequencies() {
        return frequencies;
    }
}
