package unipi.aide.mircv.model;

import unipi.aide.mircv.helpers.StreamHelper;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

public class EliasFanoCompressedList {

    private List<BitSet> highbits;
    private List<BitSet> lowbits;
    // dato che questi vettori mi servono sia per ottenere la lunghezza, che per poterli scrivere su file, preferisco calcolarli in precedenza
    private List<byte[]> highBitsAsByteArray;
    private List<byte[]> lowBitsAsByteArray;
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

    public void writeToDisk(FileOutputStream stream) throws IOException {
        StreamHelper.writeInt(stream, highbits.size());      //  write the length of highBits (how many cluster)
        for(byte[] highBits : highBitsAsByteArray){
            if(highBits.length == 0){
                highBits = new byte[1];
                Arrays.fill(highBits, (byte) 0);;
            }
            stream.write(highBits,0,highBits.length);

        }
        boolean is_first = true;
        for(byte[] lowBits : lowBitsAsByteArray){
            if(is_first) {
                StreamHelper.writeInt(stream,lowBits_len);      //num di bit per lowBit number
                is_first = false;
                if (lowBits_len == 0)
                    continue;
            }
            stream.write(lowBits,0,lowBits.length);
        }
    }

    public int getSize() {
        return highBitsAsByteArray.size() + lowbits.size() + 8;     //vengono scritti due interi non compressi per ogni postingList
    }


}
