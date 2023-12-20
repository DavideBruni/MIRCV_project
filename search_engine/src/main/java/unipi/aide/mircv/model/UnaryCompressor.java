package unipi.aide.mircv.model;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class UnaryCompressor {

    public static List<BitSet> compress(List<Integer> frequencies) {
        List<BitSet> res = new ArrayList<>();
        for(int i = 0; i< frequencies.size(); i++){
            try {
                BitSet tmp = new BitSet(i+1);
                tmp.set(1, frequencies.get(i)+1);
                res.add(tmp);
            }catch(IndexOutOfBoundsException ex){
                // if the clusters[i] is 0 (i.e. there are no numbers of that cluster), this exception will be thrown
            }
        }
        return res;
    }


    public static int writeToDisk(List<BitSet> unaryCompressedFrequencyList, FileChannel stream, int offset) throws IOException {
        for(BitSet bitSet : unaryCompressedFrequencyList){
            byte[] bits = bitSet.toByteArray();
            ByteBuffer buffer = ByteBuffer.allocateDirect(bits.length);
            buffer.put(bits);
            buffer.flip();
            offset += stream.write(buffer);
        }
        return offset;
    }

    private static int readNumber(FileChannel stream) throws IOException {
        int number = 0;
        while(true){
            ByteBuffer buffer = ByteBuffer.allocateDirect(1);
            stream.read(buffer);
            buffer.flip();
            byte frequencyBuffer = buffer.get();
            int bitsSet = Integer.bitCount(frequencyBuffer & 0xFF);
            if (bitsSet == 0)
                return number;
            number +=bitsSet;
            if(frequencyBuffer % 2 == 0)        // es: 10 --> 00000111 11111110  --> odd byte are followed by something else
                return number;
        }
    }

    public static List<Integer> readFrequencies(FileChannel freqStream, int frequencyOffset, int numberOfPostings) throws IOException {
        List<Integer> frequencies = new ArrayList<>();
        freqStream.position(frequencyOffset);
        for(int i = 0; i<numberOfPostings; i++){
            frequencies.add(readNumber(freqStream));
        }
        return frequencies;
    }
}
