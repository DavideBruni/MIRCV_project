package unipi.aide.mircv.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BitsTest {

    @Test
    void writeUnary() {
        int [] values = new int[]{3,10,1,2};
        byte [] res = new byte[3];
        long bitsoffset = 0;
        for(int value : values){
            Bits.writeUnary(res,bitsoffset,value);
            bitsoffset += value +1;
        }
        byte [] expected = new byte[]{-17, -3, 96};
        assertArrayEquals(expected,res);

    }


    @Test
    void readUnary() {
        int [] expected = new int[]{3,10,1,2};
        byte [] compressed = new byte[]{-17, -3, 96};
        int [] res = new int[4];
        long bitsoffset = 0;
        for(int i = 0; i<4; i++){
            res[i] = Bits.readUnary(compressed,bitsoffset);
            bitsoffset += res[i] + 1;
        }
        assertArrayEquals(expected,res);
    }
    @Test
    void readUnaryLimitCase() {
        int [] expected = new int[]{16,0,0,0,7};
        byte [] compressed = new byte[]{-1, -1, 15,-32};
        int [] res = new int[5];
        long bitsoffset = 0;
        for(int i = 0; i<5; i++){
            res[i] = Bits.readUnary(compressed,bitsoffset);
            bitsoffset += res[i] + 1;
        }
        assertArrayEquals(expected,res);
    }
}