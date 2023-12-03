package unipi.aide.mircv.model;

import unipi.aide.mircv.helpers.StreamHelper;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

public class EliasFanoCompressedList {

    // since I need these vectors both to obtain the length and to be able to write them to a file, I prefer to calculate them previously
    protected List<byte[]> highBitsAsByteArray;
    protected List<byte[]> lowBitsAsByteArray;
    private int lowBits_len;
    private int byteToWrite;

    public EliasFanoCompressedList(List<BitSet> highbits, List<Integer> lowbits, int l) {
        highBitsAsByteArray = new ArrayList<>();
        lowBitsAsByteArray = new ArrayList<>();
        lowBits_len = l;

        for(BitSet highbit : highbits){
            byte [] tmp = highbit.toByteArray();
            int len = tmp.length;
            if(len > 1) {
                byte[] reversedArray = new byte[len];
                for (int i = 0; i < len; i++) {
                    reversedArray[i] = tmp[len - 1 - i];
                }
                tmp = reversedArray;
            }
            highBitsAsByteArray.add(tmp);
        }
        byteToWrite = lowBits_len < 2 ? lowBits_len : (int)(Math.ceil((double)lowBits_len/8));
        for(int lowBits : lowbits){
            byte [] tmp = ByteBuffer.allocate(Integer.BYTES).putInt(lowBits).array();
            byte[] byteSetToByteArray = Arrays.copyOfRange(tmp,Integer.BYTES - byteToWrite,Integer.BYTES);
            lowBitsAsByteArray.add(byteSetToByteArray);
        }
    }

    /**
     * Writes the data of the Elias-Fano compressed docIds to the specified FileOutputStream.
     * The data includes the lengths and values of highBits and lowBits arrays.
     *
     * @param stream The FileOutputStream.
     */
    public int writeToDisk(FileOutputStream stream) throws IOException {
        StreamHelper.writeInt(stream, highBitsAsByteArray.size());      //  write the length of highBits (how many cluster)
        int offset = 4;
        for(byte[] highBits : highBitsAsByteArray){
            int len = highBits.length;
            if(len == 0){
                highBits = new byte[1];
                Arrays.fill(highBits, (byte) 0);;
                len = 1;
            }
            stream.write(highBits,0,len);
            offset += highBits.length;

        }
        StreamHelper.writeInt(stream, lowBits_len);
        offset += 4;
        if (lowBits_len != 0) {
            for (byte[] lowBits : lowBitsAsByteArray) {
                int len = lowBits.length;
                if (len < byteToWrite) {
                    byte[] tmp = new byte[byteToWrite];
                    System.arraycopy(lowBits, 0, tmp, 0, lowBits.length);
                    lowBits = tmp;
                }
                stream.write(lowBits, 0, byteToWrite);
                offset += byteToWrite;
            }
        }
        return offset;
    }

    public int getSize() {
        return highBitsAsByteArray.size() + lowBitsAsByteArray.size() + 8;     //vengono scritti due interi non compressi per ogni postingList
    }

}
