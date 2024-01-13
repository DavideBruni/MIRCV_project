package unipi.aide.mircv.model;

import org.apache.commons.lang3.ArrayUtils;
import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.DocumentNotFoundException;
import unipi.aide.mircv.queryProcessor.Scorer;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

public class CompressedPostingList extends PostingList {
    byte[] compressedIds;
    byte[] compressedFrequencies;
    private int currentStartIndexDocIds;
    private int currentStartFrequencies;
    // the following attributes are linked to the compression, they shouldn't be put in this class,
    // but at the same time they are linked to the single posting list, so they are put her for this reason
    private int previous1BitCache = -1;
    private int highBitsOffsetCache = -1;
    private int currentFrequencyIndex = 0;

    public CompressedPostingList(PostingList postingList) {
        if(postingList.blockDescriptor != null)
            blockDescriptor = new BlockDescriptor(postingList.blockDescriptor);
        else {
            if(postingList instanceof UncompressedPostingList) {
                UncompressedPostingList tmp = (UncompressedPostingList) postingList;
                blockDescriptor = new BlockDescriptor(tmp.docIds.get(tmp.docIds.size() - 1), tmp.docIds.size());
            }
            else
                throw new InvalidParameterException("Block descriptor is null and posting list passed as argument is already compressed");
        }

        if(postingList instanceof UncompressedPostingList){
            int maxDocId = blockDescriptor.getMaxDocId();
            int numberOfPostings = blockDescriptor.getNumberOfPostings();
            int compressedSize = EliasFano.getCompressedSize(maxDocId, numberOfPostings);
            compressedIds = new byte[compressedSize];
            int l = EliasFano.getL(maxDocId,numberOfPostings);
            long highBitsOffset = EliasFano.roundUp((long) l * numberOfPostings, Byte.SIZE);
            long[] docIdsOffset = new long[]{0, highBitsOffset};
            EliasFano.compress(((UncompressedPostingList) postingList).docIds, compressedIds,l,docIdsOffset);

            List<BitSet> compressedFrequenciesList = UnaryCompressor.compress(((UncompressedPostingList) postingList).frequencies);
            compressedFrequencies = new byte[0];
            for(BitSet bitSet : compressedFrequenciesList){
                byte[] bits = bitSet.toByteArray();
                compressedFrequencies = ArrayUtils.addAll(compressedFrequencies, bits);
            }
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
    }

    public static UncompressedPostingList readFromDisk(int partition, int docIdOffset, int frequencyOffset) {
        UncompressedPostingList res = null;
        try(FileChannel docStream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getRootDirectory()+TEMP_DOC_ID_DIR + "/part"+partition+".dat"),
                StandardOpenOption.READ);
            FileChannel freqStream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getRootDirectory()+TEMP_FREQ_DIR + "/part" + partition+".dat"),
                    StandardOpenOption.READ))
        {
            docStream.position(docIdOffset);
            freqStream.position(frequencyOffset);

            ByteBuffer blockInfoBuffer = ByteBuffer.allocateDirect(2*Integer.BYTES);
            docStream.read(blockInfoBuffer);
            blockInfoBuffer.flip();
            ByteBuffer nextBlockAddress = ByteBuffer.allocateDirect(Integer.BYTES);
            freqStream.read(nextBlockAddress);
            nextBlockAddress.flip();
            BlockDescriptor blockDescriptor = new BlockDescriptor(blockInfoBuffer.getInt(),blockInfoBuffer.getInt(), nextBlockAddress.getInt());

            int maxDocId = blockDescriptor.getMaxDocId();
            int numberOfPostings = blockDescriptor.getNumberOfPostings();
            ByteBuffer bufferDocIds = ByteBuffer.allocateDirect(EliasFano.getCompressedSize(maxDocId,numberOfPostings));
            int len = docStream.read(bufferDocIds);
            bufferDocIds.flip();

            byte [] buffer = new byte[len];
            bufferDocIds.get(buffer);
            List<Integer> documentIds = EliasFano.decompress(buffer,numberOfPostings,maxDocId);

            List<Integer> freqs = UnaryCompressor.readFrequencies(freqStream,frequencyOffset+Integer.BYTES,numberOfPostings);
            res = new UncompressedPostingList(documentIds,freqs,blockDescriptor);

        } catch (IOException e) {
            System.out.println("Error while retrieving posting list: "+e.getMessage());
        }
        return res;
    }

    public static int [] writeToDiskMerged(UncompressedPostingList uncompressedPostingList, FileChannel docStream, FileChannel freqStream, int[] offsets, UncompressedPostingList notWrittenYetPostings, int maxDocId, int df) throws IOException {
        int blockSize = EliasFano.getCompressedSize(maxDocId, df);
        int numBlocks = 1;
        int documentWritten = 0;
        if (blockSize > Configuration.BLOCK_TRESHOLD) {
            blockSize = (int) Math.ceil(Math.sqrt(df));
            numBlocks = uncompressedPostingList.frequencies.size() / blockSize;       // numero di blocchi presenti in questa lista
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
            }
            documentWritten = numBlocks * blockSize;
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

            docStream.position(lexiconEntry.getDocIdOffset());
            freqStream.position(lexiconEntry.getFrequencyOffset());

            ByteBuffer docIdBuffer = ByteBuffer.allocateDirect(lexiconEntry.getDocIdLength());
            ByteBuffer freqBuffer = ByteBuffer.allocateDirect(lexiconEntry.getFrequencyLength());

            byte [] docIds = new byte [lexiconEntry.getDocIdLength() - 2*Integer.BYTES];
            byte [] frequencies = new byte [lexiconEntry.getFrequencyLength() - Integer.BYTES];

            docStream.read(docIdBuffer);
            docIdBuffer.flip();

            freqStream.read(freqBuffer);
            freqBuffer.flip();
            int maxDocId = docIdBuffer.getInt();
            int n = docIdBuffer.getInt();
            BlockDescriptor firstBlockDescriptor = new BlockDescriptor(maxDocId,n,freqBuffer.getInt(),EliasFano.getCompressedSize(maxDocId,n)+8);

            docIdBuffer.get(docIds);
            freqBuffer.get(frequencies);

            res = new CompressedPostingList(docIds,frequencies,firstBlockDescriptor);
            res.lexiconEntry = lexiconEntry;
        }catch (IOException e){
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
    public void add(int docId, int frequency) {}

    @Override
    public int docId() {
        if(currentIndexPostings == Integer.MAX_VALUE)
            return Integer.MAX_VALUE;
        int[] tmp_res = EliasFano.get(Arrays.copyOfRange(compressedIds,currentStartIndexDocIds,blockDescriptor.getIndexNextBlockDocIds()),blockDescriptor.getMaxDocId(),blockDescriptor.getNumberOfPostings(), currentIndexPostings, highBitsOffsetCache, previous1BitCache);
        highBitsOffsetCache = -1;
        previous1BitCache = -1;
        return tmp_res[0];
    }

    @Override
    public double score() {
        int tf = UnaryCompressor.get(compressedIds,currentIndexPostings, currentFrequencyIndex);
        if(Configuration.getScoreStandard().equals("BM25")){
            try {
                int[] tmp_res = EliasFano.get(compressedIds,blockDescriptor.getMaxDocId(),blockDescriptor.getNumberOfPostings(),currentIndexPostings, highBitsOffsetCache, previous1BitCache);
                highBitsOffsetCache = -1;
                previous1BitCache = -1;
                return Scorer.BM25_singleTermDocumentScore(tf,tmp_res[0],lexiconEntry.getIdf());
            } catch (DocumentNotFoundException e) {
                System.out.println(e.getMessage());
                return 0;
            }
        }else{
            return Scorer.TFIDF_singleTermDocumentScore(tf,lexiconEntry.getIdf());
        }
    }

    @Override
    public void next(){
        if(++currentIndexPostings == blockDescriptor.getNumberOfPostings()){
            try{
                int start_next_block = currentStartIndexDocIds + EliasFano.getCompressedSize(blockDescriptor.getMaxDocId(),blockDescriptor.getNumberOfPostings());
                int start_next_freq_block = currentStartFrequencies + blockDescriptor.getNextFrequenciesOffset();
                blockDescriptor = new BlockDescriptor(blockDescriptor,Arrays.copyOfRange(compressedIds,start_next_block,start_next_block+8),Arrays.copyOfRange(compressedFrequencies,start_next_freq_block,start_next_freq_block+4));
                currentStartIndexDocIds = start_next_block + 8;
                currentStartFrequencies = start_next_freq_block +4;
                currentIndexPostings = 0;
                resetIndexes();
            }catch (IndexOutOfBoundsException | BufferUnderflowException e){
                currentIndexPostings = Integer.MAX_VALUE;
            }
        }
    }

    @Override
    public void nextGEQ(int docId) {
        if(docId() >= docId){
            return;
        }
        if(blockDescriptor.getMaxDocId() >= docId){
            currentIndexPostings = EliasFano.nextGEQ(Arrays.copyOfRange(compressedIds,currentStartIndexDocIds,blockDescriptor.getIndexNextBlockDocIds()),blockDescriptor.getNumberOfPostings(),blockDescriptor.getMaxDocId(),docId);
        }else{
            try {
                while (true) {
                    try{
                        int start_next_block = currentStartIndexDocIds + EliasFano.getCompressedSize(blockDescriptor.getMaxDocId(),blockDescriptor.getNumberOfPostings());
                        int start_next_freq_block = currentStartFrequencies + blockDescriptor.getNextFrequenciesOffset();
                        blockDescriptor = new BlockDescriptor(blockDescriptor,Arrays.copyOfRange(compressedIds,start_next_block,start_next_block+8),Arrays.copyOfRange(compressedFrequencies,start_next_freq_block,start_next_freq_block+4));
                        currentStartIndexDocIds = start_next_block + 8;
                        currentStartFrequencies = start_next_freq_block +4;
                        if(blockDescriptor.getMaxDocId() >=docId){
                            currentIndexPostings = EliasFano.nextGEQ(Arrays.copyOfRange(compressedIds,currentStartIndexDocIds,blockDescriptor.getIndexNextBlockDocIds()),blockDescriptor.getNumberOfPostings(),blockDescriptor.getMaxDocId(),docId);
                            resetIndexes();
                            break;
                        }
                    }catch (IndexOutOfBoundsException ie){
                        currentIndexPostings = Integer.MAX_VALUE;
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
        highBitsOffsetCache = previous1BitCache = -1;
    }

}
