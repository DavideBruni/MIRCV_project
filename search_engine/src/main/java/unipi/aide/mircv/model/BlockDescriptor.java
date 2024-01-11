package unipi.aide.mircv.model;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class BlockDescriptor {
    private final int maxDocId;
    private final int numberOfPostings;
    private final int nextFrequenciesOffset;

    public BlockDescriptor(BlockDescriptor blockDescriptor) {
        maxDocId = blockDescriptor.maxDocId;
        numberOfPostings = blockDescriptor.numberOfPostings;
        nextFrequenciesOffset = blockDescriptor.nextFrequenciesOffset;
    }

    public BlockDescriptor(int maxDocId, int numberOfPostings) {
        this.maxDocId = maxDocId;
        this.numberOfPostings = numberOfPostings;
        nextFrequenciesOffset = -1;
    }

    public BlockDescriptor(int maxDocId, int numberOfPostings, int nextFrequenciesOffset) {
        this.maxDocId = maxDocId;
        this.numberOfPostings = numberOfPostings;
        this.nextFrequenciesOffset = nextFrequenciesOffset;
    }

    public int getMaxDocId() {return maxDocId; }

    public int getNumberOfPostings() {return numberOfPostings; }

    public int writeOnDisk(FileChannel docStream) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(2 * Integer.BYTES);
        buffer.putInt(maxDocId);
        buffer.putInt(numberOfPostings);
        buffer.flip();
        return docStream.write(buffer);
    }
}
