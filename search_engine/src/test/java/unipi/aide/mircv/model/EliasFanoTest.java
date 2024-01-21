package unipi.aide.mircv.model;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EliasFanoTest {

    @Test
    void getL() {
        // TODO commento sul test e su come abbiamo ottenuto 2
        int l = EliasFano.getL(12, 4);
        assertEquals(2,l);
    }

    @Test
    void testGetL(){
        int l = EliasFano.getL(128,5);
        int l2 = (int) Math.ceil(Math.log(128/(double)5)/Math.log(2));
        assertEquals(l,l2);
    }

    @Test
    void getLSingleNumber() {
        // TODO commento sul test e su come abbiamo ottenuto 3
        int l = EliasFano.getL(5, 1);
        assertEquals(3,l);
    }

    @Test
    void getLSameNumberOfU() {
        // TODO commento sul test e su come abbiamo ottenuto 0
        int l = EliasFano.getL(1200, 1200);
        assertEquals(0,l);
    }

    @Test
    void compress() {
        // TODO commento sul test e su come abbiamo ottenuto il risultato
        List<Integer> integerList = new ArrayList<>();
        integerList.add(5);
        integerList.add(8);
        integerList.add(9);
        integerList.add(12);
        byte [] compressed = new byte[2];
        long highBitsOffset = EliasFano.roundUp(8);
        EliasFano.compress(integerList,compressed,2,highBitsOffset);
        byte [] result = new byte[]{68,90};
        assertArrayEquals(result,compressed);
    }

    @Test
    void decompress() {
        byte [] input = new byte[]{68,90};
        List<Integer> res = EliasFano.decompress(input,4,12);
        List<Integer> expected = new ArrayList<>();
        expected.add(5);
        expected.add(8);
        expected.add(9);
        expected.add(12);
        assertIterableEquals(expected,res);
    }

    @Test
    void getCompressedSize() {
        List<Integer> integerList = new ArrayList<>();
        integerList.add(5);
        integerList.add(8);
        integerList.add(9);
        integerList.add(12);
        int res = EliasFano.getCompressedSize(12,4);
        assertEquals(2,res);
    }

    @Test
    void get() {
        // TODO add comment
        byte [] input = new byte[]{68,90};
        int [] res = EliasFano.get(input,12,4,1,-1,-1);
        assertArrayEquals(new int[]{8,1,0},res);
    }

    @Test
    void nextGEQ() {
        byte [] input = new byte[]{68,90};
        int res = EliasFano.nextGEQ(input,4,12,10);
        assertEquals(3,res);
    }

    @Test
    void nextGEQNotAvailable() {
        byte [] input = new byte[]{68,90};
        int res = EliasFano.nextGEQ(input,4,12,18);
        assertEquals(-1,res);
    }
}