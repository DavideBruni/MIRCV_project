package unipi.aide.mircv.model;

import unipi.aide.mircv.helpers.StreamHelper;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

public class EliasFanoCompressedList {

    protected List<BitSet> highbits;
    protected List<BitSet> lowbits;
    // since I need these vectors both to obtain the length and to be able to write them to a file, I prefer to calculate them previously
    protected List<byte[]> highBitsAsByteArray;
    protected List<byte[]> lowBitsAsByteArray;
    private int lowBits_len;

    public EliasFanoCompressedList(List<BitSet> highbits, List<BitSet> lowbits, int l) {
        this.highbits = highbits;
        this.lowbits = lowbits;
        highBitsAsByteArray = new ArrayList<>();
        lowBitsAsByteArray = new ArrayList<>();
        lowBits_len = l;

        for(BitSet highbit : highbits){
            highBitsAsByteArray.add(highbit.toByteArray());
        }
        for(BitSet lowBit : lowbits){
            lowBitsAsByteArray.add(lowBit.toByteArray());
        }
    }

    /**
     * Writes the data of the Elias-Fano compressed docIds to the specified FileOutputStream.
     * The data includes the lengths and values of highBits and lowBits arrays.
     *
     * @param stream The FileOutputStream.
     */
    public int writeToDisk(FileOutputStream stream) throws IOException {
        StreamHelper.writeInt(stream, highbits.size());      //  write the length of highBits (how many cluster)
        int offset = 4;
        for(byte[] highBits : highBitsAsByteArray){
            if(highBits.length == 0){
                highBits = new byte[1];
                Arrays.fill(highBits, (byte) 0);;
            }
            stream.write(highBits,0,highBits.length);
            offset += highBits.length;

        }
        boolean is_first = true;
        for(byte[] lowBits : lowBitsAsByteArray){
            if(is_first) {
                StreamHelper.writeInt(stream,lowBits_len);      //num di bit per lowBit number
                offset += 4;
                is_first = false;
                if (lowBits_len == 0)
                    break;
            }
            if(lowBits.length == 0){
                lowBits = new byte[1];
                Arrays.fill(lowBits, (byte) 0);;
            }
            stream.write(lowBits,0,lowBits.length);
            offset += lowBits.length;
        }
        return offset;
    }

    public int getSize() {
        return highBitsAsByteArray.size() + lowbits.size() + 8;     //vengono scritti due interi non compressi per ogni postingList
    }

}
