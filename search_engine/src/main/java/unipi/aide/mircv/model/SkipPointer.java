package unipi.aide.mircv.model;

import unipi.aide.mircv.exceptions.DocIdNotFoundException;

import java.io.*;
import java.util.List;

public class SkipPointer {
    private long maxDocId;
    private int docIdsOffset;
    private int frequencyOffset;
    private int numDocId;
    private static final int SKIP_POINTER_DIMENSION = 8 + 4 + 4 + 4;
    private static final String SKIP_POINTERS_PATH = "data/skip_pointers.dat";
    private static int BYTE_WRITTEN = 0;

    public SkipPointer(long docid, int docIdsOffset, int frequencyOffset, int numDocId) {
        maxDocId = docid;
        this.docIdsOffset = docIdsOffset;
        this.frequencyOffset = frequencyOffset;
        this.numDocId = numDocId;
    }

    public SkipPointer() {

    }

    public static int write(List<SkipPointer> skippingPointers, LexiconEntry lexiconEntry) {
        try(DataOutputStream docStream = new DataOutputStream(new FileOutputStream(SKIP_POINTERS_PATH, true))){
            lexiconEntry.setSkipPointerOffset(BYTE_WRITTEN);
            for(SkipPointer skipPointer : skippingPointers){
                docStream.writeLong(skipPointer.maxDocId);
                docStream.writeInt(skipPointer.docIdsOffset);
                docStream.writeInt(skipPointer.frequencyOffset);
                docStream.writeInt(skipPointer.numDocId);
                BYTE_WRITTEN += SKIP_POINTER_DIMENSION;
            }
        } catch (IOException e) {
            // handle error in some way
            return 1;
        }
        return skippingPointers.size();
    }

    public static SkipPointer readFromDisk(int skipPointerOffset, int numBlockRead) throws IOException {
        DataInputStream docStream = new DataInputStream(new FileInputStream(SKIP_POINTERS_PATH));
        docStream.skipBytes(skipPointerOffset + numBlockRead* SKIP_POINTER_DIMENSION);
        return new SkipPointer(docStream.readLong(),docStream.readInt(),docStream.readInt(),docStream.readInt());
    }

    public void setDocIdOffset(int i) {
    }

    public void setFrequencyOffset(int i) {
    }

    public void setNumberOfDocId(int size) {
        numDocId = size;
    }

    public long getMaxDocId() {
        return maxDocId;
    }

    public void setMaxDocId(long maxDocId) {
        this.maxDocId = maxDocId;
    }

    public int getDocIdsOffset() {
        return docIdsOffset;
    }

    public void setDocIdsOffset(int docIdsOffset) {
        this.docIdsOffset = docIdsOffset;
    }

    public int getFrequencyOffset() {
        return frequencyOffset;
    }

    public int getNumDocId() {
        return numDocId;
    }

    public void setNumDocId(int numDocId) {
        this.numDocId = numDocId;
    }

    public static int getByteWritten() {
        return BYTE_WRITTEN;
    }

    public static void setByteWritten(int byteWritten) {
        BYTE_WRITTEN = byteWritten;
    }

    // altro non sarà che un file che contiene i descrittori dei blocchi della posting list, nel caso in cui ce ne sia più di uno
}
