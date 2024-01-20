package unipi.aide.mircv.model;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.DocumentNotFoundException;
import unipi.aide.mircv.log.CustomLogger;
import unipi.aide.mircv.queryProcessor.Scorer;

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
    private int frequenciesIndex;
    private int startNextFrequencyBlock;

    public UncompressedPostingList(int[] docIdsArray, int[] freqArray, BlockDescriptor blockDescriptor) {
        this.docIds = new ArrayList<>();
        this.frequencies = new ArrayList<>();
        for(int i = 0; i<docIdsArray.length; i++){
            docIds.add(docIdsArray[i]);
            frequencies.add(freqArray[i]);
        }
        this.blockDescriptor = new BlockDescriptor(blockDescriptor);
    }

    public UncompressedPostingList(List<Integer> documentIds, List<Integer> freqs, BlockDescriptor blockDescriptor) {
        docIds = documentIds;
        frequencies = freqs;
        this.blockDescriptor = new BlockDescriptor(blockDescriptor);
    }

    public UncompressedPostingList() {
        docIds = new ArrayList<>();
        frequencies = new ArrayList<>();
    }

    public UncompressedPostingList(List<Integer> blockDocIds, List<Integer> blockFrequencies) {
        blockDescriptor = new BlockDescriptor(blockDocIds.get(blockDocIds.size()-1),blockDocIds.size());
        docIds = new ArrayList<>();
        docIds.addAll(blockDocIds);
        frequencies = new ArrayList<>();
        frequencies.addAll(blockFrequencies);
    }

    public UncompressedPostingList(byte[] docIds, byte[] frequencies, BlockDescriptor firstBlockDescriptor) {
        this.docIds = new ArrayList<>();
        this.frequencies = new ArrayList<>();
        ByteBuffer docIdBuffer = ByteBuffer.wrap(docIds);
        ByteBuffer freqBuffer = ByteBuffer.wrap(frequencies);

        while(true){
            try{
                this.docIds.add(docIdBuffer.getInt());
            }catch (Exception e){
                break;
            }
        }
        while(true){
            try{
                this.frequencies.add(freqBuffer.getInt());
            }catch (Exception e){
                break;
            }
        }

        this.blockDescriptor = new BlockDescriptor(firstBlockDescriptor);
        startNextFrequencyBlock = this.blockDescriptor.getNumberOfPostings();
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
            docStream.read(blockInfoBuffer);
            blockInfoBuffer.flip();
            BlockDescriptor blockDescriptor = new BlockDescriptor(blockInfoBuffer.getInt(),blockInfoBuffer.getInt());

            int numberOfPosting = blockDescriptor.getNumberOfPostings();
            ByteBuffer bufferDocIds = ByteBuffer.allocateDirect(Integer.BYTES*numberOfPosting);
            ByteBuffer bufferFrequencies = ByteBuffer.allocateDirect(Integer.BYTES*numberOfPosting);
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

            docStream.position(lexiconEntry.getDocIdOffset());
            freqStream.position(lexiconEntry.getFrequencyOffset());

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
        if(currentIndexPostings<docIds.size()){
            return docIds.get(currentIndexPostings);
        }else{
            return Integer.MAX_VALUE;
        }
    }

    @Override
    public double score() {
        if(Configuration.getScoreStandard().equals("BM25")){
            try {
                return Scorer.BM25_singleTermDocumentScore(frequencies.get(frequenciesIndex),docIds.get(currentIndexPostings),lexiconEntry.getIdf());
            } catch (DocumentNotFoundException e) {
                System.out.println(e.getMessage());
                return 0;
            }
        }else{
            return Scorer.TFIDF_singleTermDocumentScore(frequencies.get(frequenciesIndex), lexiconEntry.getIdf());
        }
    }

    @Override
    public void next(){
        if(++currentIndexPostings == blockDescriptor.getNumberOfPostings()){
            try{
                // numberOfPosting is used as index limiter, so we have to consider also the 2 int of the block descriptor and the previous number of postings
                blockDescriptor = new BlockDescriptor(docIds.get(currentIndexPostings++), 1 + currentIndexPostings + docIds.get(currentIndexPostings));
                frequenciesIndex++;
                startNextFrequencyBlock += docIds.get(currentIndexPostings++);

            }catch (IndexOutOfBoundsException ie){
                currentIndexPostings = Integer.MAX_VALUE;
                frequenciesIndex = Integer.MAX_VALUE;
            }
        }else{
            frequenciesIndex++;
        }
    }

    @Override
    public void nextGEQ(int docId) {
        if(docId() >= docId){
            return;
        }
        if(blockDescriptor.getMaxDocId() >= docId){
            for(;currentIndexPostings<blockDescriptor.getNumberOfPostings(); currentIndexPostings++,frequenciesIndex++){
                if(docIds.get(currentIndexPostings) >= docId)
                    return;
            }
        }else{
            while (true) {
                try{
                    currentIndexPostings = blockDescriptor.getNumberOfPostings();
                    frequenciesIndex = startNextFrequencyBlock;
                    int blockMaxId = docIds.get(currentIndexPostings++);
                    blockDescriptor = new BlockDescriptor(blockMaxId,2 + blockDescriptor.getNumberOfPostings() + docIds.get(currentIndexPostings));
                    startNextFrequencyBlock += docIds.get(currentIndexPostings++);
                    if(blockMaxId >= docId){
                        for(; currentIndexPostings<blockDescriptor.getNumberOfPostings(); currentIndexPostings++,frequenciesIndex++){
                            if(docIds.get(currentIndexPostings) >= docId)
                                return;
                        }
                    }
                }catch (IndexOutOfBoundsException ie){
                    currentIndexPostings = Integer.MAX_VALUE;
                    frequenciesIndex = Integer.MAX_VALUE;
                    break;
                }
            }

        }
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
        offsets[1] += freqStream.write(freqBuffer);

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
                if(numBlocks == 0 && notWrittenYetPostings == null){        // need to write the last part of block
                    blockSize = docIds.size();
                    numBlocks = 1;
                }
            }
            if(!docIds.isEmpty() && !(docIds.size() < blockSize && notWrittenYetPostings != null)) {
                // if not empty and (I have more than one block to be written or it's the last one)
                for (int j = 0; j < numBlocks; j++) {
                    List<Integer> blockDocIds = docIds.subList(j * blockSize, Math.min((j + 1) * blockSize,docIds.size()));
                    List<Integer> blockFrequencies = frequencies.subList(j * blockSize, Math.min((j + 1) * blockSize,frequencies.size()));
                    UncompressedPostingList tmp = new UncompressedPostingList(blockDocIds,blockFrequencies);
                    offsets = tmp.writeOnDisk(docStream,freqStream,offsets);
                    documentWritten +=blockDocIds.size();
                }
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

    public List<Integer> getDocIds() {
        return docIds;
    }

    public List<Integer> getFrequencies() {
        return frequencies;
    }

    public void setBlockDescriptor() {
        blockDescriptor = new BlockDescriptor(docIds.get(docIds.size()-1),docIds.size());
    }
}
