package unipi.aide.mircv.model;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class UnaryCompressor {

    public static List<BitSet> compress(int[] clusters) {
        List<BitSet> res = new ArrayList<>();
        for(int i = 0; i<clusters.length; i++){        // since 0 must be represente as 0, I have to n bit
            BitSet tmp = new BitSet(i+1);
            try {
                tmp.set(1, clusters[i]+1);          //the L.S.b. must be 0
            }catch(IndexOutOfBoundsException ex){
                // if the clusters[i] is 0 (i.e. there are no numbers of that cluster), this exception will be thrown
            }
            res.add(tmp);
        }

        return res;
    }

    public static List<BitSet> compress(List<Posting> postingList) {
        List<BitSet> res = new ArrayList<>();
        for(int i = 0; i< postingList.size(); i++){
            try {
                BitSet tmp = new BitSet(i+1);
                tmp.set(1, postingList.get(i).frequency+1);
                res.add(tmp);
            }catch(IndexOutOfBoundsException ex){
                // if the clusters[i] is 0 (i.e. there are no numbers of that cluster), this exception will be thrown
            }
        }
        return res;
    }


    public static int writeToDisk(List<BitSet> unaryCompressedFrequencyList, int offset, FileOutputStream stream) throws IOException {
        for(BitSet bitSet : unaryCompressedFrequencyList){
            byte[] bits = bitSet.toByteArray();
            stream.write(bits,0,bits.length);
            offset += bits.length;
        }
        return offset;
    }

    public static int readNumber(InputStream docStream) throws IOException {
        int number = 0;
        while(true){
            byte[] buffer = docStream.readNBytes(1);
            int bitsSet = Integer.bitCount(buffer[0] & 0xFF);
            if (bitsSet == 0)
                return number;
            number +=bitsSet;
            if(bitsSet < 8)
                return number;
        }
    }

    public static List<Integer> readFrequencies(FileInputStream freqStream, int frequencyOffset, int numberOfPostings) throws IOException {
        List<Integer> frequencies = new ArrayList<>();
        freqStream.skipNBytes(frequencyOffset);
        for(int i = 0; i<numberOfPostings; i++){
            frequencies.add(readNumber(freqStream));
        }
        return frequencies;
    }
}
