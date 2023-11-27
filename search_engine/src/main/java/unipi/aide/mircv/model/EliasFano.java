package unipi.aide.mircv.model;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class EliasFano {
    public static EliasFanoCompressedList compress(List<Posting> postingList) {
        // greatest id to represent, why + 1? If I had as highest number a multiple of 2, it gives as number of bits needed
        // to repreresent the number one bit less than the necessary, for example if I had to store [15,16] I need 3 bit for lower bits
        // and 2 for higher bits, without the plus 1, formulas gave me 1 bit for high bits
        int U = postingList.get(postingList.size() - 1).docid + 1;
        int n = postingList.size();
        int l = (int) Math.ceil(Math.log(U/n)/Math.log(2));
        int sizeHighBits = ((int) Math.ceil(Math.log(U)/Math.log(2))) - l;
        int numHighBits = (int) Math.pow(2, sizeHighBits);
        List<BitSet> lowbits = new ArrayList<>();

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
        highbits = UnaryCompressor.compress(clusters);


        return new EliasFanoCompressedList(highbits,lowbits,l);

    }

    public static List<Integer> decompress(InputStream docStream, long docIdOffset) throws IOException {
        List<Integer> docIds = new ArrayList<>();
        byte[] integer_buffer = new byte[4];
        docStream.skipNBytes(docIdOffset);
        docStream.readNBytes(integer_buffer,0,Integer.BYTES);       // leggo l'intero
        int highBitsLen = byteArrayToInt(integer_buffer);                     // lo convergo
        int[] clusters = new int[highBitsLen];                          // creo un array che contiene i cluster
        for(int i = 0; i<highBitsLen; i++){
            clusters[i] = UnaryCompressor.readNumber(docStream);
        }

        int numBitPerLowBits = byteArrayToInt(docStream.readNBytes(Integer.BYTES));
        for(int i = 0; i<highBitsLen; i++){
            for(int j = 0; j<clusters[i]; j++){
                int byteWritten = numBitPerLowBits == 1 ? 1 : (int)(Math.ceil((Math.log(numBitPerLowBits)/Math.log(2))/8));
                byte[] lowBits = new byte[0];
                if(byteWritten > 0){
                    lowBits = docStream.readNBytes(byteWritten);
                }
                docIds.add(decompressNumber(i,lowBits,numBitPerLowBits));
            }
        }

        return docIds;
    }

    private static int decompressNumber(int j, byte[] lowBits, int numBitPerLowBits) {
        int docId = j << numBitPerLowBits;
        int lowBitsAsInt = byteArrayToInt(lowBits);
        docId |= lowBitsAsInt;  // Imposta gli ultimi numeroBit di target con quelli di sorgente
        return docId;
    }

    private static int byteArrayToInt(byte[] byteArray) {
        byte[] bytesToConvert = new byte[Integer.BYTES];
        if (byteArray.length != 4) {
            System.arraycopy(byteArray, 0, bytesToConvert, Integer.BYTES - byteArray.length, byteArray.length);
        }else{
            bytesToConvert = byteArray;
        }

        // Convert from big-endian to integer
        int result = 0;
        for (int i = 0; i < 4; i++) {
            result = (result << 8) | (bytesToConvert[i] & 0xFF);
        }

        return result;
    }


}
