package unipi.aide.mircv.model;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class BlockDescriptor {
    private final int maxDocId;
    private final int numberOfPostings;
    // only the previous value are written in the documentIds file on disk
    private final int nextFrequenciesOffset;    // Faster way to get the exact size of frequencies block
    private final int indexNextBlockDocIds;     // Faster way to get the exact size of docIds block

    public BlockDescriptor(BlockDescriptor blockDescriptor) {
        maxDocId = blockDescriptor.maxDocId;
        numberOfPostings = blockDescriptor.numberOfPostings;
        nextFrequenciesOffset = blockDescriptor.nextFrequenciesOffset;
        indexNextBlockDocIds = blockDescriptor.indexNextBlockDocIds;
    }

    public BlockDescriptor(int maxDocId, int numberOfPostings) {
        this.maxDocId = maxDocId;
        this.numberOfPostings = numberOfPostings;
        nextFrequenciesOffset = -1;
        indexNextBlockDocIds = -1;
    }

    public BlockDescriptor(int maxDocId, int numberOfPostings, int nextFrequenciesOffset) {
        this.maxDocId = maxDocId;
        this.numberOfPostings = numberOfPostings;
        this.nextFrequenciesOffset = nextFrequenciesOffset;
        this.indexNextBlockDocIds = 0;
    }

    public BlockDescriptor(int maxDocId, int numberOfPostings, int nextFrequenciesOffset, int indexNextBlockDocIds) {
        this.maxDocId = maxDocId;
        this.numberOfPostings = numberOfPostings;
        this.nextFrequenciesOffset = nextFrequenciesOffset;
        this.indexNextBlockDocIds = indexNextBlockDocIds;
    }

    public BlockDescriptor(BlockDescriptor blockDescriptor, byte[] docIdsDescriptor, byte[] frequenciesDescriptor) {
        ByteBuffer docBuffer = ByteBuffer.wrap(docIdsDescriptor);
        maxDocId = docBuffer.getInt();
        if(maxDocId==0){        //Something is wrong, when I copied the array, I used index greater than the actual length
            numberOfPostings = nextFrequenciesOffset = indexNextBlockDocIds =0;
            return;
        }
        numberOfPostings = docBuffer.getInt();
        // start of next blockId is: currentStart + sizeOfTheBlock + sizeOfCurrentBlockDescriptor (i.e. 8)
        indexNextBlockDocIds = blockDescriptor.indexNextBlockDocIds + EliasFano.getCompressedSize(maxDocId,numberOfPostings) + 8;

        ByteBuffer freqBuffer = ByteBuffer.wrap(frequenciesDescriptor);
        // start of next frequencies is: currentStart + sizeOfTheBlock (i.e. getInt) + sizeOfCurrentBlockDescriptor (i.e. 8)
        nextFrequenciesOffset = blockDescriptor.nextFrequenciesOffset + freqBuffer.getInt() + 4;
    }

    public int getMaxDocId() {return maxDocId; }

    public int getNumberOfPostings() {return numberOfPostings; }

    public int getNextFrequenciesOffset() {
        return nextFrequenciesOffset;
    }

    public int getIndexNextBlockDocIds() {
        return indexNextBlockDocIds;
    }

    public int writeOnDisk(FileChannel docStream) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(2 * Integer.BYTES);
        buffer.putInt(maxDocId);
        buffer.putInt(numberOfPostings);
        buffer.flip();
        return docStream.write(buffer);
    }

    public int getIndexNextBlockFrequencies() {
        return nextFrequenciesOffset;
    }
}
