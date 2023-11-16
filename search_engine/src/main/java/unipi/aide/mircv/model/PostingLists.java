package unipi.aide.mircv.model;

import unipi.aide.mircv.exceptions.PostingListStoreException;
import unipi.aide.mircv.helpers.FileHelper;
import unipi.aide.mircv.log.CustomLogger;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class PostingLists {

    Map<String, PostingList> postings;                  // for each token, we have it's posting list
    private static final String TEMP_DOC_ID_DIR ="data/invertedIndex/temp/docIds";
    private static final String TEMP_FREQ_DIR ="data/invertedIndex/temp/frequencies";
    private static int NUM_FILE_WRITTEN = 0;       // since partial posting lists are stored in different partition, we need to know how many of them
    private static final int POSTING_SIZE_THRESHOLD = 2048;      //2KB

    public PostingLists() { this.postings = new HashMap<>(); }

    /**
     * Adds a posting to the inverted index of a given token.
     * The Posting containing the provided document ID and frequency is then added to the PostingList.
     *
     * @param docId     The document ID to the posting list.
     * @param token     The token of the posting list.
     * @param frequency The frequency of the token in the document.
     */
    public void add(long docId, String token, int frequency) {
        if (!postings.containsKey(token)){
            postings.put(token, new PostingList());     // If the specified token is not present in the postings, a new PostingList is created.
        }
        // add the new Posting to the token posting list
        postings.get(token).add(new Posting(docId, frequency));
    }

    /**
     * Merges multiple PostingLists associated with the same token into a single PostingList.
     *
     * @param postingLists A list of PostingLists to be merged.
     * @param token        The token associated with the PostingList.
     */
    public void add(List<PostingList> postingLists, String token) {
        List<Posting> postings_merged = new ArrayList<>();
        for(PostingList postingList : postingLists){
            postings_merged.addAll(postingList.postingList);
        }
        // Sort the merged PostingList in ascending order
        postings_merged.sort(Comparator.comparingLong(Posting::getDocid));
        // The resulting PostingList is then associated with the specified token
        postings.put(token, new PostingList(postings_merged,token));
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
        FileHelper.createDir(TEMP_DOC_ID_DIR);
        FileHelper.createDir(TEMP_FREQ_DIR);

        try (FileOutputStream docStream = new FileOutputStream(TEMP_DOC_ID_DIR + "/part" + NUM_FILE_WRITTEN + ".dat");
             FileOutputStream freqStream = new FileOutputStream(TEMP_FREQ_DIR + "/part" + NUM_FILE_WRITTEN + ".dat")){
            if (!compressed){
                try(DataOutputStream docStream_dos = new DataOutputStream(docStream);
                    DataOutputStream freqStream_dos = new DataOutputStream(freqStream)) {
                        writeToDiskNotCompressed(docStream_dos, freqStream_dos, 0, false);
                }
            }else{
                writeToDiskCompressed(docStream, freqStream,0, 0, false);
            }
            NUM_FILE_WRITTEN++;
        } catch (IOException e) {
            CustomLogger.error("Error while writing posting lists on disk: abort operation");
            throw new PostingListStoreException();
        }

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

    public int[] writeToDiskCompressed(FileOutputStream docStream, FileOutputStream freqStream, int docOffset, int freqOffset, boolean is_merged) throws IOException {
        int[] offsets = new int[]{docOffset,freqOffset};

        CustomLogger.info("Start writing posting list...");
        for(String token: postings.keySet()){
            List<List<Posting>> postingListsToCompress = new ArrayList<>();
            List<Posting> postingLists = postings.get(token).getPostingList();      //get the posting list for a token

            long U = postingLists.get(postingLists.size() - 1).docid;       // greatest long to be stored
            long n = postingLists.size();                                   // how many posting must be stored

            // Need Skipping Pointer
            LexiconEntry lexiconEntry = Lexicon.getEntry(token);
            List<SkipPointer> skipPointers = new ArrayList<>();

            // check if we are writing the final posting list for the token (is merged)
            // check if the size of compressed docIds will be above the POSTING_SIZE_TRESHOLD
            if(is_merged && ((n*Math.ceil(Math.log(U/(double)n) / Math.log(2.0)) +2*n)/8 > POSTING_SIZE_THRESHOLD)){
                CustomLogger.info("Initialize skipping pointers");
                // initialize skipping pointers using heuristically optimal block size
                int blockSize = (int) Math.round(Math.sqrt(postings.get(token).getPostingList().size()));
                initializeSkipPointers(blockSize,postingListsToCompress,skipPointers,postingLists);
            }else{
                // create anyway a list of list in order to use a unique method to write and
                // compressed posting list both if it's divided in blocks or not
                postingListsToCompress.add(postingLists);
            }
                offsets = compressAndWritePostingList(postingListsToCompress,skipPointers,lexiconEntry,docStream,freqStream, docOffset, freqOffset);

            if(skipPointers.size() > 1) { // check if we have skipping pointers
                int numBlocks = SkipPointer.write(skipPointers, lexiconEntry);  // write them on disk
                lexiconEntry.setNumBlocks(numBlocks);           // update lexicon entry
            }
            Lexicon.setEntry(token,lexiconEntry);

        }
        CustomLogger.info("...posting list stored");
        return offsets;

    }

    /**
     *
     * @param postingListsToCompress A list of list of postings to be compressed and written.
     * @param skipPointers           The list of skip pointers for skip compression. May be empty if no skip pointers are used.
     * @param lexiconEntry           The lexicon entry associated with the posting lists.
     * @param docStream              The output stream for document IDs.
     * @param freqStream             The output stream for frequencies.
     * @param docOffset              The initial offset for writing document IDs in the output stream.
     * @param freqOffset             The initial offset for writing frequencies in the output stream.
     * @return An array of two integers representing the final offsets after writing the compressed data.
     *         The first element is the offset for document IDs, and the second element is the offset for frequencies.
     */
    private int[] compressAndWritePostingList(List<List<Posting>> postingListsToCompress, List<SkipPointer> skipPointers, LexiconEntry lexiconEntry, FileOutputStream docStream, FileOutputStream freqStream, int docOffset, int freqOffset) throws IOException {
        lexiconEntry.setDocIdOffset(docOffset);
        lexiconEntry.setFrequencyOffset(freqOffset);

        // if posting list is not divided in blocks, this for have a single iteration
        for(List<Posting> postingList : postingListsToCompress){
            CustomLogger.info("Compressing document ids");
            EliasFanoCompressedList eliasFanoCompressedDocIdList = EliasFano.compress(postingList);
            CustomLogger.info("Compressing term frequencies");
            List<BitSet> unaryCompressedFrequencyList = UnaryCompressor.compress(postingList);

            CustomLogger.info("Writing compressed document ids");
            if(skipPointers.size()>0)
                skipPointers.get(postingListsToCompress.indexOf(postingList)).setDocIdOffset(docOffset);
            eliasFanoCompressedDocIdList.writeToDisk(docStream);
            docOffset = eliasFanoCompressedDocIdList.getSize();

            CustomLogger.info("Writing compressed term frequencies");
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


    /**
     * Writes the posting lists to the specified output streams without compression.
     *
     * @param docIdStream     The output stream for document IDs.
     * @param frequencyStream The output stream for frequencies.
     * @param offset          The initial offset for output streams.
     * @param is_merged        Indicates whether the posting lists have been merged for potential skip pointers.
     * @return The final offset after writing the uncompressed data, representing the number of postings written.
     */
    public int writeToDiskNotCompressed(DataOutputStream docIdStream, DataOutputStream frequencyStream, int offset, boolean is_merged) throws IOException {
        CustomLogger.info("Start writing posting list...");
        for(String token: postings.keySet()){
            List<Posting> postingLists = postings.get(token).getPostingList();
            Lexicon.updateDocIdOffset(token, offset * Long.BYTES);
            Lexicon.updateFrequencyOffset(token, offset * Integer.BYTES);
            // check posting list dimension (if is the final one) to eventually create skipping-pointers
            if (is_merged && postingLists.size() * (Long.BYTES + Integer.BYTES) > POSTING_SIZE_THRESHOLD){
                CustomLogger.info("Generating skipping pointers");
                LexiconEntry lexiconEntry = Lexicon.getEntry(token);
                addSkipPointers(token, lexiconEntry);       // we can create them directly, we have fixed dimension for docIds and frequencies
                Lexicon.setEntry(token,lexiconEntry);
            }
            for (Posting posting : postingLists) {              //for each posting in postingList
                docIdStream.writeLong(posting.docid);           // writing docId
                frequencyStream.writeInt(posting.frequency);    // writing frequency
                offset++;
            }
            Lexicon.setNumberOfPostings(token,postingLists.size());     //used for retrieve not compressed posting lists from disk
        }
        try {
            docIdStream.close();
            frequencyStream.close();
        }catch(IOException e){
            CustomLogger.error("Error while closing streams");
        }
        CustomLogger.info("...posting list stored");
        return offset;      // is not an offset, but the number of posting lists written

    }


    /**
     * Adds skip pointers to the Lexicon entry for the given token based on the posting list.
     *
     * @param token         The token for which skip pointers are being added.
     * @param lexiconEntry  The lexicon entry associated with the token.
     */
    private void addSkipPointers(String token, LexiconEntry lexiconEntry) {
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
