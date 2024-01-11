package unipi.aide.mircv.model;

import org.apache.commons.lang3.ArrayUtils;
import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.log.CustomLogger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class CompressedPostingList extends PostingList {
    byte[] compressedIds;
    byte[] compressedFrequencies;

    public CompressedPostingList() {}

    public CompressedPostingList(PostingList postingList) {
        blockDescriptor = new BlockDescriptor(postingList.blockDescriptor);

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
        currentIndex = 0;
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
            docStream.position(docIdOffset);
            freqStream.position(frequencyOffset);
            ByteBuffer blockInfoBuffer = ByteBuffer.allocateDirect(2*Integer.BYTES);
            docStream.read(blockInfoBuffer);
            blockInfoBuffer.flip();
            BlockDescriptor blockDescriptor = new BlockDescriptor(blockInfoBuffer.getInt(),blockInfoBuffer.getInt());
            int maxDocId = blockDescriptor.getMaxDocId();
            int numberOfPostings = blockDescriptor.getNumberOfPostings();
            ByteBuffer bufferDocIds = ByteBuffer.allocateDirect(EliasFano.getCompressedSize(maxDocId,numberOfPostings));
            int len = docStream.read(bufferDocIds);
            bufferDocIds.flip();

            byte [] buffer = new byte[len];
            bufferDocIds.get(buffer);
            List<Integer> documentIds = EliasFano.decompress(buffer,numberOfPostings,maxDocId);
            List<Integer> freqs = UnaryCompressor.readFrequencies(freqStream,frequencyOffset,numberOfPostings);
            res = new UncompressedPostingList(documentIds,freqs,blockDescriptor);

        } catch (IOException e) {
            CustomLogger.error("Error while retrieving posting list: "+e.getMessage());
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

            ByteBuffer docIdBuffer = ByteBuffer.allocateDirect(lexiconEntry.getDocIdLength());
            ByteBuffer freqBuffer = ByteBuffer.allocateDirect(lexiconEntry.getFrequencyLength());

            byte [] docIds = new byte [lexiconEntry.getDocIdLength() - 2*Integer.BYTES];
            byte [] frequencies = new byte [lexiconEntry.getFrequencyLength() - Integer.BYTES];


            docStream.read(docIdBuffer);
            docIdBuffer.flip();

            freqStream.read(freqBuffer);
            freqBuffer.flip();

            BlockDescriptor firstBlockDescriptor = new BlockDescriptor(docIdBuffer.getInt(),docIdBuffer.getInt(),freqBuffer.getInt());

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

        ByteBuffer freqBuffer = ByteBuffer.allocateDirect(compressedFrequencies.length);
        freqBuffer.put(compressedFrequencies);
        freqBuffer.flip();
        offsets[1] += docStream.write(freqBuffer);

        return offsets;
    }

    @Override
    public void add(int docId, int frequency) {

    }

    @Override
    public int docId() {
        return 0;
    }

    @Override
    public double score() {
        return 0;
    }

    @Override
    public void next() throws IOException {

    }

    @Override
    public void nextGEQ(int docId) {

    }

}
