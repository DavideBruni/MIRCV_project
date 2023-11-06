package unipi.aide.mircv.model;

import java.io.*;
import java.util.List;

public class SkipPointer {
    private long maxDocId;
    private int docIdsOffset;
    private int frequencyOffset;
    private static final int skipPointerDimension = 8 + 4 + 4;
    private static final String SKIP_POINTERS_PATH = "data/skip_pointers.dat";
    private static int NUM_SKIP_POINTER = 0;
    private static int BYTE_WRITTEN = 0;

    public SkipPointer(long docid, int docIdsOffset, int frequencyOffset) {
        maxDocId = docid;
        this.docIdsOffset = docIdsOffset;
        this.frequencyOffset = frequencyOffset;
    }

    public static int write(List<SkipPointer> skippingPointers, LexiconEntry lexiconEntry) {
        try(DataOutputStream docStream = new DataOutputStream(new FileOutputStream(SKIP_POINTERS_PATH, true))){
            lexiconEntry.setSkipPointerOffset(BYTE_WRITTEN);
            for(SkipPointer skipPointer : skippingPointers){
                docStream.writeLong(skipPointer.maxDocId);
                docStream.writeInt(skipPointer.docIdsOffset);
                docStream.writeInt(skipPointer.frequencyOffset);
                docStream.writeInt(skipPointer.skipPointerDimension);
                BYTE_WRITTEN +=skipPointerDimension;
            }
        } catch (IOException e) {
            // handle error in some way
            return 1;
        }
        return skippingPointers.size();
    }


    // altro non sarà che un file che contiene i descrittori dei blocchi della posting list, nel caso in cui ce ne sia più di uno
}
