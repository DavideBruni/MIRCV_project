package unipi.aide.mircv.model;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.helpers.StreamHelper;

import java.io.*;
import java.util.List;

public class SkipPointer {
    private int maxDocId;
    private int docIdsOffset;
    private int frequencyOffset;
    private int numDocId;
    private static final int SKIP_POINTER_DIMENSION = 4 + 4 + 4 + 4;
    private static int BYTE_WRITTEN = 0;

    public SkipPointer(int docid, int docIdsOffset, int frequencyOffset, int numDocId) {
        maxDocId = docid;
        this.docIdsOffset = docIdsOffset;
        this.frequencyOffset = frequencyOffset;
        this.numDocId = numDocId;
    }

    public SkipPointer() {

    }

    public static int write(List<SkipPointer> skippingPointers, LexiconEntry lexiconEntry) {
        String filePath = Configuration.getSkipPointersPath();
        StreamHelper.createDir(filePath);

        filePath = filePath + "/skip_pointers.dat";
        try(DataOutputStream docStream = new DataOutputStream(new FileOutputStream(filePath, true))){
            lexiconEntry.setSkipPointerOffset(BYTE_WRITTEN);
            for(SkipPointer skipPointer : skippingPointers){
                docStream.writeInt(skipPointer.maxDocId);
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
        String filePath = Configuration.getSkipPointersPath();
        filePath = filePath + "/skip_pointers.dat";
        DataInputStream docStream = new DataInputStream(new FileInputStream(filePath));
        docStream.skipBytes(skipPointerOffset + numBlockRead*SKIP_POINTER_DIMENSION);
        SkipPointer tmp =  new SkipPointer(docStream.readInt(),docStream.readInt(),docStream.readInt(),docStream.readInt());
        docStream.close();
        return tmp;
    }

    public void setDocIdOffset(int i) {
    }

    public void setFrequencyOffset(int i) {
    }

    public void setNumberOfDocId(int size) {
        numDocId = size;
    }

    public int getMaxDocId() {
        return maxDocId;
    }

    public int getDocIdsOffset() {
        return docIdsOffset;
    }

    public int getFrequencyOffset() {
        return frequencyOffset;
    }

    public int getNumDocId() {
        return numDocId;
    }

    public void setMaxDocId(int docid) {
        maxDocId = docid;
    }
}
