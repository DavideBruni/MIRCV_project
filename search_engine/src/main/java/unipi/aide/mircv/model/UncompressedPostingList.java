package unipi.aide.mircv.model;

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
import java.util.List;

public class UncompressedPostingList extends PostingList{
    List<Integer> docIds;
    List<Integer> frequencies;
    

    public UncompressedPostingList(int[] docIdsArray, int[] freqArray, BlockDescriptor blockDescriptor) {
        super();
    }

    public UncompressedPostingList(List<Integer> documentIds, List<Integer> freqs, BlockDescriptor blockDescriptor) {
        super();
    }

    public UncompressedPostingList() {
        docIds = new ArrayList<>();
        frequencies = new ArrayList<>();
    }

    public UncompressedPostingList(List<Integer> blockDocIds, List<Integer> blockFrequencies) {
        blockDescriptor = new BlockDescriptor(blockDocIds.get(blockDocIds.size()-1),blockDocIds.size());
        docIds = blockDocIds;
        frequencies = blockFrequencies;
    }

    public UncompressedPostingList(byte[] docIds, byte[] frequencies, BlockDescriptor firstBlockDescriptor) {
        super();
    }

    public static UncompressedPostingList readFromDisk(int partition, int docIdOffset, int frequencyOffset) throws FileNotFoundException {
        UncompressedPostingList res = null;
        try(FileChannel docStream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getRootDirectory()+TEMP_DOC_ID_DIR + "/part"+partition+".dat"),
                StandardOpenOption.READ);
            FileChannel freqStream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getRootDirectory()+TEMP_FREQ_DIR + "/part" + partition+".dat"),
                    StandardOpenOption.READ))
        {
            docStream.position(docIdOffset);
            freqStream.position(frequencyOffset);
            ByteBuffer blockInfoBuffer = ByteBuffer.allocateDirect(2*Integer.BYTES);
            BlockDescriptor blockDescriptor = new BlockDescriptor(blockInfoBuffer.getInt(),blockInfoBuffer.getInt());
            int numberOfPosting = blockDescriptor.getNumberOfPostings();
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
            res = new UncompressedPostingList(docIdsArray,freqArray,blockDescriptor);
            
        } catch (IOException e) {
            CustomLogger.error("Error while retrieving posting list: "+e.getMessage());
        }
        return res;
    }

    public static PostingList loadFromDisk(LexiconEntry lexiconEntry) {
        PostingList res = null;
        try (FileChannel docStream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getDocumentIdsPath()));
             FileChannel freqStream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getFrequencyPath()))){

            ByteBuffer docIdBuffer = ByteBuffer.allocateDirect(lexiconEntry.getDocIdLength());
            ByteBuffer freqBuffer = ByteBuffer.allocateDirect(lexiconEntry.getFrequencyLength());

            byte [] docIds = new byte [lexiconEntry.getDocIdLength() - 2*Integer.BYTES];
            byte [] frequencies = new byte [lexiconEntry.getFrequencyLength()];


            docStream.read(docIdBuffer);
            docIdBuffer.flip();

            freqStream.read(freqBuffer);
            freqBuffer.flip();

            BlockDescriptor firstBlockDescriptor = new BlockDescriptor(docIdBuffer.getInt(),docIdBuffer.getInt());

            docIdBuffer.get(docIds);
            freqBuffer.get(frequencies);

            res = new UncompressedPostingList(docIds,frequencies,firstBlockDescriptor);
            res.lexiconEntry = lexiconEntry;
        }catch (IOException e){
        }
        return res;
    }

    @Override
    public void add(int docId, int frequency) {
        docIds.add(docId);
        frequencies.add(frequency);
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

    @Override
    public int[] writeOnDisk(FileChannel docStream, FileChannel freqStream, int[] offsets) throws IOException {
        offsets[0] += blockDescriptor.writeOnDisk(docStream);
        ByteBuffer docIdBuffer = ByteBuffer.allocateDirect(Integer.BYTES * docIds.size());
        ByteBuffer freqBuffer = ByteBuffer.allocateDirect(Integer.BYTES * frequencies.size());
        for(int i = 0; i<docIds.size(); i++){
            docIdBuffer.putInt(docIds.get(i));
            freqBuffer.putInt(frequencies.get(i));
        }
        docIdBuffer.flip();
        freqBuffer.flip();
        offsets[0] += docStream.write(docIdBuffer);
        offsets[1] += docStream.write(freqBuffer);

        return offsets;
    }

    public int[] writeToDiskMerged(FileChannel docStream, FileChannel freqStream, int[] offsets, int df, UncompressedPostingList notWrittenYetPostings,int maxDocId) throws IOException {
        if(notWrittenYetPostings != null && !notWrittenYetPostings.docIds.isEmpty()){
            docIds.addAll(0,notWrittenYetPostings.docIds);
            frequencies.addAll(0,notWrittenYetPostings.frequencies);
        }
        int blockSize;
        int numBlocks;
        int documentWritten = 0;
        if(Configuration.isCOMPRESSED()) {
            return CompressedPostingList.writeToDiskMerged(this, docStream, freqStream, offsets, notWrittenYetPostings, maxDocId,df);
        }else {
            blockSize = df;
            numBlocks = 1;
            if (blockSize * Integer.BYTES > Configuration.BLOCK_TRESHOLD) {
                blockSize = (int) Math.sqrt(df);
                numBlocks = docIds.size() / blockSize;
            }
            if(!docIds.isEmpty() && !(docIds.size() < blockSize && notWrittenYetPostings != null)) {
                // if not empty and (I have more than one block to be written or it's the last one)
                for (int j = 0; j < numBlocks; j++) {
                    List<Integer> blockDocIds = docIds.subList(j * blockSize, (j + 1) * blockSize);
                    List<Integer> blockFrequencies = frequencies.subList(j * blockSize, (j + 1) * blockSize);
                    UncompressedPostingList tmp = new UncompressedPostingList(blockDocIds,blockFrequencies);
                    offsets = tmp.writeOnDisk(docStream,freqStream,offsets);
                }
                documentWritten = numBlocks * blockSize;
            }
        }
        if(notWrittenYetPostings != null) {
            if(documentWritten<docIds.size()){
                notWrittenYetPostings.docIds = docIds.subList(documentWritten,docIds.size());
                notWrittenYetPostings.frequencies = frequencies.subList(documentWritten,frequencies.size());
            }else {
                notWrittenYetPostings.docIds = new ArrayList<>();
                notWrittenYetPostings.frequencies = new ArrayList<>();
            }
        }
        return offsets;
    }

}
