package unipi.aide.mircv.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class EliasFanoTest {


    static Stream<Arguments> parametersGetLTest() {
        return Stream.of(
                Arguments.of(32, 7, 3),
                Arguments.of(32, 8, 2),
                Arguments.of(127, 8, 4),
                Arguments.of(5, 1, 3),
                Arguments.of(1200, 1200, 0)
        );
    }
    @ParameterizedTest
    @MethodSource("parametersGetLTest")
    void getL(int u, int len, int expected) {
        int l = EliasFano.getL(u, len);
        assertEquals(expected,l);
    }

    @Test
    void compress() {
        List<Integer> integerList = new ArrayList<>();
        integerList.add(16);
        integerList.add(18);
        integerList.add(24);
        integerList.add(25);
        integerList.add(26);
        integerList.add(30);
        integerList.add(32);
        byte [] expected = new byte[]{ 0b00001000, 0b00010101, (byte) 0b10000000, 0b00110111, (byte) 0b10100000};
        byte [] compressed = new byte[5];
        long highBitsOffset = EliasFano.roundUp(3 * 7);
        EliasFano.compress(integerList,compressed,3,highBitsOffset);
        assertArrayEquals(expected,compressed);
    }

    @Test
    void compressLowBitsGreaterThan1Byte() {
        List<Integer> integerList = new ArrayList<>();
        integerList.add(1024);
        integerList.add(1025);
        integerList.add(10000);
        integerList.add(10001);
        int l = EliasFano.getL(10001, 4);
        byte [] expected = new byte[]{ 64, 4, 1, 113, 7, 17, -52};
        byte [] compressed = new byte[EliasFano.getCompressedSize(10001, 4)];
        long highBitsOffset = EliasFano.roundUp(l * 4L);
        EliasFano.compress(integerList,compressed,l,highBitsOffset);
        assertArrayEquals(expected,compressed);
    }

    @Test
    void compressHighBitsGreaterThan1Byte() {
        List<Integer> integerList = new ArrayList<>();
        integerList.add(1);
        integerList.add(2);
        integerList.add(3);
        integerList.add(4);
        integerList.add(5);
        integerList.add(6);
        integerList.add(7);
        integerList.add(8);
        integerList.add(9);
        integerList.add(10);
        integerList.add(512);
        int res = EliasFano.getCompressedSize(512,11);
        int l = EliasFano.getL(512, 11);
        byte [] expected = new byte[]{ 4, 32, -60, 20, 97, -56, 36, -96, 0, -1, -64, 32};
        byte [] compressed = new byte[res];
        long highBitsOffset = EliasFano.roundUp(l * 11L);
        EliasFano.compress(integerList,compressed,l,highBitsOffset);
        assertArrayEquals(expected,compressed);
    }

    static Stream<Arguments> parametersDecompress() {
        List<Integer> integerList1 = new ArrayList<>();
        integerList1.add(1);
        integerList1.add(2);
        integerList1.add(3);
        integerList1.add(4);
        integerList1.add(5);
        integerList1.add(6);
        integerList1.add(7);
        integerList1.add(8);
        integerList1.add(9);
        integerList1.add(10);
        integerList1.add(512);
        List<Integer> integerList2 = new ArrayList<>();
        integerList2.add(1024);
        integerList2.add(1025);
        integerList2.add(10000);
        integerList2.add(10001);
        List<Integer> integerList3 = new ArrayList<>();
        integerList3.add(16);
        integerList3.add(18);
        integerList3.add(24);
        integerList3.add(25);
        integerList3.add(26);
        integerList3.add(30);
        integerList3.add(32);
        return Stream.of(
                Arguments.of(new byte[]{ 4, 32, -60, 20, 97, -56, 36, -96, 0, -1, -64, 32}, 512, 11,integerList1),
                Arguments.of(new byte[]{ 64, 4, 1, 113, 7, 17, -52}, 10001, 4,integerList2),
                Arguments.of(new byte[]{ 8, 21, -128, 55, -96}, 32, 7,integerList3)
        );
    }

    @ParameterizedTest
    @MethodSource("parametersDecompress")
    void decompress(byte []  compressed, int u, int n, ArrayList<Integer> expected) {
        List<Integer> res = EliasFano.decompress(compressed,n,u);
        assertIterableEquals(expected,res);
    }

    @Test
    void getCompressedSize() {
        int res = EliasFano.getCompressedSize(12,4);
        assertEquals(2,res);
    }

    @Test
    void getTest1() {
        EliasFanoCache cache = new EliasFanoCache();
        byte [] compress = new byte[]{ 8, 21, -128, 55, -96};
        int id = EliasFano.get(compress,32,7,0,cache);
        assertEquals(16,id);
    }

    @Test
    void getTest2() {
        EliasFanoCache cache = new EliasFanoCache();
        byte [] compress = new byte[]{ 4, 32, -60, 20, 97, -56, 36, -96, 0, -1, -64, 32};
        int id = EliasFano.get(compress,512,11,10,cache);
        assertEquals(512,id);
    }

    @Test
    void getTest3() {
        EliasFanoCache cache = new EliasFanoCache();
        byte [] compress = new byte[]{ 8, 21, -128, 55, -96};
        int id = EliasFano.get(compress,32,7,2,cache);
        assertEquals(24,id);
    }

    @Test
    void getTest4() {
        EliasFanoCache cache = new EliasFanoCache();
        byte [] compress = new byte[]{ 8, 21, -128, 55, -96};
        int id = EliasFano.get(compress,32,7,8,cache);
        assertEquals(Integer.MAX_VALUE,id);
    }


}