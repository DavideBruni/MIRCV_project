package unipi.aide.mircv.model;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.DocumentNotFoundException;
import unipi.aide.mircv.queryProcessor.Scorer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CompressedPostingList extends PostingList {
    byte[] compressedIds;
    byte[] compressedFrequencies;
    private int currentStartIndexDocIds;
    private int currentStartFrequencies;

    // the following attributes are linked to the compression, they shouldn't be put in this class,
    // but at the same time they are linked to the single posting list, so they are put her for this reason
    private EliasFanoCache cache;
    private long currentFrequencyIndex = 0;
    private int lastFrequencyRead = -1;

    /* Constructors */
    public CompressedPostingList(PostingList postingList) {
        cache = new EliasFanoCache();
        /* ----------------------- Copy or create a new block descriptor -----------------------*/
        if(postingList.blockDescriptor != null)
            blockDescriptor = new BlockDescriptor(postingList.blockDescriptor);
        else {
            if(postingList instanceof UncompressedPostingList) {
                UncompressedPostingList tmp = (UncompressedPostingList) postingList;
                blockDescriptor = new BlockDescriptor(tmp.getMaxDocId(), tmp.docIds.size());
            }
            else
                throw new InvalidParameterException("Block descriptor is null and posting list passed as argument is already compressed");
                // it means that I called this constructor with a compressed postingList without a blockDescriptor and this is a useless situation
                // I throw an exception to indicate an error situation
        }
        /* ----------------------- End block descriptor -----------------------*/
        if(postingList instanceof UncompressedPostingList){
            // Compress a posting list
            /* ------------ Information needed to compress with Elias-Fano----------*/
            int maxDocId = blockDescriptor.getMaxDocId();
            int numberOfPostings = blockDescriptor.getNumberOfPostings();
            int compressedSize = EliasFano.getCompressedSize(maxDocId, numberOfPostings);
            compressedIds = new byte[compressedSize];
            int l = EliasFano.getL(maxDocId,numberOfPostings);
            long highBitsOffset = EliasFano.roundUp((long) l * numberOfPostings);
            /* ---------------------------------------------------------------------*/

            EliasFano.compress(((UncompressedPostingList) postingList).docIds, compressedIds,l,highBitsOffset);
            compressedFrequencies = UnaryCompressor.compress(((UncompressedPostingList) postingList).frequencies);

        }else{
            compressedIds = ((CompressedPostingList) postingList).compressedIds.clone();
            compressedFrequencies = ((CompressedPostingList) postingList).compressedFrequencies.clone();
        }
    }

    public CompressedPostingList(byte[] docIds, byte[] frequencies, BlockDescriptor firstBlockDescriptor) {
        compressedIds = docIds;
        compressedFrequencies = frequencies;
        blockDescriptor = firstBlockDescriptor;
        currentIndexPostings = 0;
        currentStartIndexDocIds = 0;
        cache = new EliasFanoCache();
    }

    /* END CONSTRUCTORS */

    public static UncompressedPostingList readFromDisk(int partition, int docIdOffset, int frequencyOffset) {
        UncompressedPostingList res = null;
        try(FileChannel docStream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getRootDirectory()+TEMP_DOC_ID_DIR + "/part"+partition+".dat"),
                StandardOpenOption.READ);
            FileChannel freqStream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getRootDirectory()+TEMP_FREQ_DIR + "/part" + partition+".dat"),
                    StandardOpenOption.READ))
        {
            // ------------------- Move the cursor to the right position ------------------
            docStream.position(docIdOffset);
            freqStream.position(frequencyOffset);

            // --------------------- read and create the BlockDescriptor (only 1 block) -------------
            ByteBuffer blockInfoBuffer = ByteBuffer.allocateDirect(2*Integer.BYTES);
            docStream.read(blockInfoBuffer);
            blockInfoBuffer.flip();
            ByteBuffer frequencyBlockLength = ByteBuffer.allocateDirect(Integer.BYTES);
            freqStream.read(frequencyBlockLength);
            frequencyBlockLength.flip();
            BlockDescriptor blockDescriptor = new BlockDescriptor(blockInfoBuffer.getInt(),blockInfoBuffer.getInt(), frequencyBlockLength.getInt());

            // -------------------------- Read Document Ids ---------------
            int maxDocId = blockDescriptor.getMaxDocId();
            int numberOfPostings = blockDescriptor.getNumberOfPostings();
            ByteBuffer bufferDocIds = ByteBuffer.allocateDirect(EliasFano.getCompressedSize(maxDocId,numberOfPostings));
            int len = docStream.read(bufferDocIds);
            bufferDocIds.flip();

            byte [] buffer = new byte[len];
            bufferDocIds.get(buffer);
            List<Integer> documentIds = EliasFano.decompress(buffer,numberOfPostings,maxDocId);
            // -------------------------- End of reading ids --------------------------

            // -------------------------- Read frequencies -------------------------
            ByteBuffer frequenciesBuffer = ByteBuffer.allocateDirect(blockDescriptor.getNextFrequenciesOffset());
            len = freqStream.read(frequenciesBuffer);
            frequenciesBuffer.flip();
            buffer = new byte[len];
            frequenciesBuffer.get(buffer);
            List<Integer> freqs = UnaryCompressor.decompressFrequencies(buffer, blockDescriptor.getNumberOfPostings());
            // -------------------------- End of reading frequencies --------------------------
            res = new UncompressedPostingList(documentIds,freqs,blockDescriptor);

        } catch (IOException e) {
            System.out.println("Error while retrieving posting list: "+e.getMessage());
        }
        return res;
    }

    public static int [] writeToDiskMerged(UncompressedPostingList uncompressedPostingList, FileChannel docStream, FileChannel freqStream, int[] offsets, UncompressedPostingList notWrittenYetPostings, int maxDocId, int df) throws IOException {
        int blockSize = EliasFano.getCompressedSize(maxDocId, df);      // what should be the compressed size if I had 1 single block
        int numBlocks = 1;
        int documentWritten = 0;
        // blockSize is used as number of Byte to check the condition, after this id is used as number of posting per block
        if (blockSize > Configuration.BLOCK_TRESHOLD) {     // use skipping pointers
            if(notWrittenYetPostings ==  null){             // I have to write the last block
                blockSize = uncompressedPostingList.docIds.size();      // I have to write it all
            }else {
                blockSize = (int) Math.sqrt(df);            // choose this as blockSize
                numBlocks = uncompressedPostingList.frequencies.size() / blockSize;       // number of blocks (truncated) present in this partial posting list
            }
        } else {
            blockSize = df;
        }
        if(!uncompressedPostingList.docIds.isEmpty() && (uncompressedPostingList.docIds.size() >= blockSize || notWrittenYetPostings == null)) {
            // if not empty and (I have more than one block to be written or it's the last one)
            for (int j = 0; j < numBlocks; j++) {
                List<Integer> docIds = uncompressedPostingList.docIds.subList(j * blockSize, (j + 1) * blockSize);
                List<Integer> frequencies = uncompressedPostingList.frequencies.subList(j * blockSize, (j + 1) * blockSize);
                UncompressedPostingList tmp = new UncompressedPostingList(docIds,frequencies);
                CompressedPostingList compressedPostingList = new CompressedPostingList(tmp);
                offsets = compressedPostingList.writeOnDisk(docStream,freqStream,offsets);
                documentWritten +=docIds.size();
            }
        }
        if(notWrittenYetPostings != null) {
            if(documentWritten<uncompressedPostingList.docIds.size()){
                notWrittenYetPostings.docIds = uncompressedPostingList.docIds.subList(documentWritten,uncompressedPostingList.docIds.size());
                notWrittenYetPostings.frequencies = uncompressedPostingList.frequencies.subList(documentWritten,uncompressedPostingList.frequencies.size());
            }else {
                // I have written all the possible documents
                notWrittenYetPostings.docIds = new ArrayList<>();
                notWrittenYetPostings.frequencies = new ArrayList<>();
            }
        }
        return offsets;
    }

    public static PostingList loadFromDisk(LexiconEntry lexiconEntry) {
        PostingList res = null;
        try (FileChannel docStream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getDocumentIdsPath()));
             FileChannel freqStream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getFrequencyPath()))){

            // Move the cursor to the right position
            docStream.position(lexiconEntry.getDocIdOffset());
            freqStream.position(lexiconEntry.getFrequencyOffset());

            // docId and frequency length consider also the information written before the different blocks
            ByteBuffer docIdBuffer = ByteBuffer.allocateDirect(lexiconEntry.getDocIdLength());
            ByteBuffer freqBuffer = ByteBuffer.allocateDirect(lexiconEntry.getFrequencyLength());

            docStream.read(docIdBuffer);
            docIdBuffer.flip();

            freqStream.read(freqBuffer);
            freqBuffer.flip();

            // putting in the following arrays what I read except the first block descriptor (the other ones are inside the arrays)
            byte [] docIds = new byte [lexiconEntry.getDocIdLength() - 2*Integer.BYTES];
            byte [] frequencies = new byte [lexiconEntry.getFrequencyLength() - Integer.BYTES];

            // create first block descriptor
            int maxDocId = docIdBuffer.getInt();
            int n = docIdBuffer.getInt();
            BlockDescriptor firstBlockDescriptor = new BlockDescriptor(maxDocId,n,freqBuffer.getInt(),EliasFano.getCompressedSize(maxDocId,n));

            docIdBuffer.get(docIds);
            freqBuffer.get(frequencies);

            res = new CompressedPostingList(docIds,frequencies,firstBlockDescriptor);
            // associate the lexiconEntry to the PostingList in order to have a simpler way to access to some information
            res.lexiconEntry = lexiconEntry;
        }catch (IOException e){
            System.out.println("error");
        }

        return res;
    }

    @Override
    public int[] writeOnDisk(FileChannel docStream, FileChannel freqStream, int[] offsets) throws IOException {
        offsets[0] += blockDescriptor.writeOnDisk(docStream);

        ByteBuffer docIdBuffer = ByteBuffer.allocateDirect(compressedIds.length);
        docIdBuffer.put(compressedIds);
        docIdBuffer.flip();
        offsets[0] += docStream.write(docIdBuffer);

        ByteBuffer blockFrequenciesInfo = ByteBuffer.allocateDirect(Integer.BYTES);
        blockFrequenciesInfo.putInt(compressedFrequencies.length);
        blockFrequenciesInfo.flip();
        offsets[1] += freqStream.write(blockFrequenciesInfo);

        ByteBuffer freqBuffer = ByteBuffer.allocateDirect(compressedFrequencies.length);
        freqBuffer.put(compressedFrequencies);
        freqBuffer.flip();
        offsets[1] += freqStream.write(freqBuffer);

        return offsets;
    }

    @Override
    public void add(int docId, int frequency) {}      // I cannot add a compressed element, but I have to implement due to abstract superclass

    @Override
    public int docId() {
        if(currentIndexPostings == Integer.MAX_VALUE || currentIndexPostings == -1)
            return Integer.MAX_VALUE;
        // I pass to the function only the current block, with the necessary information to decompress the current docId
        return EliasFano.get(
                Arrays.copyOfRange(compressedIds,currentStartIndexDocIds,blockDescriptor.getIndexNextBlockDocIds()),
                blockDescriptor.getMaxDocId(),blockDescriptor.getNumberOfPostings(),
                currentIndexPostings,cache);
        //cache is an instance of EliasFanoCache, contains some useful information to avoid repetition of some operation
    }

    @Override
    public double score() {
        if(currentIndexPostings == Integer.MAX_VALUE || currentIndexPostings == -1)
            return 0.0;
        /* ---------------------------- GET TF -----------------------*/
        /*          tf = res [0]; newOffset = res[1]                  */
        long[] res= UnaryCompressor.get(Arrays.copyOfRange(compressedFrequencies,currentStartFrequencies,
                blockDescriptor.getIndexNextBlockFrequencies()),currentIndexPostings, lastFrequencyRead, currentFrequencyIndex);
        /* -----------------------------------------------------------*/
        currentFrequencyIndex = res[1];             //update the index of frequencies compressed as unary
        lastFrequencyRead = currentIndexPostings;   // the last frequency read was related to currentIndexPosting
        if(Configuration.getScoreStandard().equals("BM25")){
            try {
                int tmp_res = EliasFano.get(Arrays.copyOfRange(compressedIds,currentStartIndexDocIds,
                        blockDescriptor.getIndexNextBlockDocIds()),blockDescriptor.getMaxDocId(),blockDescriptor.getNumberOfPostings(),
                        currentIndexPostings, cache);
                return Scorer.BM25_singleTermDocumentScore((int)res[0],tmp_res,lexiconEntry.getDf());
            } catch (DocumentNotFoundException e) {
                System.out.println(e.getMessage());
                return 0;
            }
        }else{
            return Scorer.TFIDF_singleTermDocumentScore((int)res[0],lexiconEntry.getDf());
        }
    }

    @Override
    public void next(){
        if(currentIndexPostings == Integer.MAX_VALUE)
            return;
        if(++currentIndexPostings == blockDescriptor.getNumberOfPostings()){
                int start_next_block = blockDescriptor.getIndexNextBlockDocIds();
                // I was on last block, from now on, used as value an impossible one
                if(start_next_block >= compressedIds.length){
                    currentIndexPostings = Integer.MAX_VALUE;
                    currentFrequencyIndex = Integer.MAX_VALUE;
                    return;
                }
                int start_next_freq_block = blockDescriptor.getNextFrequenciesOffset();
                blockDescriptor = new BlockDescriptor(blockDescriptor,Arrays.copyOfRange(compressedIds,start_next_block,start_next_block+8),Arrays.copyOfRange(compressedFrequencies,start_next_freq_block,start_next_freq_block+4));
                if (blockDescriptor.getMaxDocId()==0) {
                    currentIndexPostings = Integer.MAX_VALUE;
                    currentFrequencyIndex = Integer.MAX_VALUE;
                    return;
                }
                // I consider as start of the block the position after the block information
                currentStartIndexDocIds = start_next_block + 8;
                currentStartFrequencies = start_next_freq_block +4;
                currentIndexPostings = 0;
                resetIndexes();     //reset indexes about frequency array
        }
    }

    @Override
    public void nextGEQ(int docId) {
        if(docId() >= docId){       //the current docId is greater than the searched one
            return;
        }
        // if I have at least one docId >= docId
        if(blockDescriptor.getMaxDocId() >= docId){
            while (true) {      // linear search
                int id = EliasFano.get(
                        Arrays.copyOfRange(compressedIds,currentStartIndexDocIds,blockDescriptor.getIndexNextBlockDocIds()),
                        blockDescriptor.getMaxDocId(),blockDescriptor.getNumberOfPostings(),
                        ++currentIndexPostings,cache);
                if (id >= docId)
                    break;
            }
        }else{
            try {       // use skipping pointers
                while (true) {
                        /* --------------- LOAD BLOCK DESCRIPTOR ------------- */
                        int start_next_block = blockDescriptor.getIndexNextBlockDocIds();
                        int start_next_freq_block = blockDescriptor.getNextFrequenciesOffset();
                        blockDescriptor = new BlockDescriptor(blockDescriptor,Arrays.copyOfRange(compressedIds,start_next_block,start_next_block+8),Arrays.copyOfRange(compressedFrequencies,start_next_freq_block,start_next_freq_block+4));
                        // if last block is already analyzed
                        if (blockDescriptor.getMaxDocId()==0) {
                            currentIndexPostings = Integer.MAX_VALUE;
                            break;
                        }
                        /* -------------------------------------------------- */
                        /* --------------- UPDATE START INDEXES ------------- */
                        currentStartIndexDocIds = start_next_block + 8;
                        currentStartFrequencies = start_next_freq_block +  4;
                        /* -------------------------------------------------- */
                        if(blockDescriptor.getMaxDocId() >=docId){
                            currentIndexPostings = 0;
                            resetIndexes();
                            nextGEQ(docId);
                            break;
                        }
                }
            }catch (IndexOutOfBoundsException ie){
                currentIndexPostings = Integer.MAX_VALUE;
            }
        }
    }

    private void resetIndexes() {
        currentFrequencyIndex = 0;
        lastFrequencyRead =  -1;
        cache = new EliasFanoCache();
    }

}
