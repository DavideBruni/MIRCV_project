package unipi.aide.mircv.model;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class EliasFano {
    public static EliasFanoCompressedList compress(List<Posting> postingList) {
        long U = postingList.get(postingList.size() - 1).docid;     // greatest long id to represent
        long n = postingList.size();
        int l = (int) Math.ceil(Math.log(U/n)/Math.log(2));
        int sizeHighBits = ((int) Math.ceil(Math.log(U)/Math.log(2))) - l;
        int numHighBits = (int) Math.pow(2, sizeHighBits);
        List<BitSet> lowbits = new ArrayList<>();           // l'array normale non supporta long come numero di elementi

        int clusters [] = new int[numHighBits];
        List<BitSet> highbits = new ArrayList();

        for(Posting posting : postingList){
            BitSet lowBitsSet = new BitSet(l);
            for (int i = 0; i < l; i++) {
                boolean bit = ((posting.docid >> i) & 1) == 1;
                lowBitsSet.set(i, bit);
            }
            lowbits.add(lowBitsSet);

            // high bits: set clusters
            int tmp = (int) (posting.docid >> l);
            clusters[tmp]++;

        }
        // high bits: Unary code
        highbits = UnaryCompressor.compress(clusters,sizeHighBits);


        return new EliasFanoCompressedList(highbits,lowbits,l);

    }

    public static List<Long> decompress(FileInputStream docStream, int docIdOffset) throws IOException {
        List<Long> docIds = new ArrayList<>();
        byte[] integer_buffer = new byte[4];
        docStream.readNBytes(integer_buffer,docIdOffset,Integer.BYTES);       // leggo l'intero
        int highBitsLen = byteArrayToInt(integer_buffer);                     // lo convergo
        int[] clusters = new int[highBitsLen];                          // creo un array che contiene i cluster
        for(int i = 0; i<highBitsLen; i++){
            clusters[i] = UnaryCompressor.readNumber(docStream);
        }

        int numBitPerLowBits = byteArrayToInt(docStream.readNBytes(Integer.BYTES));
        for(int i = 0; i<highBitsLen; i++){
            for(int j = 0; j<clusters[i]; j++){
                int byteWritten = (int)(Math.ceil((Math.log(numBitPerLowBits)/Math.log(2))/8));
                byte[] lowBits = new byte[0];
                if(byteWritten > 0){
                    lowBits = docStream.readNBytes(byteWritten);
                }
                docIds.add(decompressNumber(i,lowBits,numBitPerLowBits));
            }
        }

        return docIds;
    }

    private static long decompressNumber(int j, byte[] lowBits, int numBitPerLowBits) {
        long docId = (long) j << numBitPerLowBits;
        long lowBitsAsLong = byteArrayToLong(lowBits);
        docId |= lowBitsAsLong;  // Imposta gli ultimi numeroBit di target con quelli di sorgente
        return docId;
    }

    private static int byteArrayToInt(byte[] byteArray) {
        if (byteArray.length != 4) {
            throw new IllegalArgumentException("Byte array must have exactly 4 elements");
        }

        // Convert from big-endian to integer
        int result = 0;
        for (int i = 0; i < 4; i++) {
            result = (result << 8) | (byteArray[i] & 0xFF);
        }

        return result;
    }

    private static long byteArrayToLong(byte[] byteArray) {
        byte[] bytesToConvert = new byte[Long.BYTES];
        if (byteArray.length != 8) {
            System.arraycopy(byteArray, 0, bytesToConvert, Long.BYTES - byteArray.length, byteArray.length);
        }else{
            bytesToConvert = byteArray;
        }

        // Convert from big-endian to integer
        int result = 0;
        for (int i = 0; i < 8; i++) {
            result = (result << 8) | (bytesToConvert[i] & 0xFF);
        }

        return result;
    }

}
