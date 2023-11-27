package unipi.aide.mircv.model;
import org.junit.jupiter.api.Test;
import unipi.aide.mircv.helpers.StreamHelper;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.*;

class EliasFanoTest {

    @Test
    void testConversionCorrectness(){
        // Prepare test data
        List<Posting> postingList = Arrays.asList(
                new Posting(10, 3),
                new Posting(15, 2),
                new Posting(16, 4),
                new Posting(18, 5),
                new Posting(27, 10)
        );

        // Compress the posting list
        EliasFanoCompressedList compressedList = EliasFano.compress(postingList);
        List<byte[]> expectedHighBits = new ArrayList<>();
        expectedHighBits.add(new byte[]{});
        expectedHighBits.add(new byte[]{6});
        expectedHighBits.add(new byte[]{6});
        expectedHighBits.add(new byte[]{2});
        List<byte[]> expectedLowBits = new ArrayList<>();
        expectedLowBits.add(new byte[]{2});
        expectedLowBits.add(new byte[]{7});
        expectedLowBits.add(new byte[]{});
        expectedLowBits.add(new byte[]{2});
        expectedLowBits.add(new byte[]{3});

        // Compare highBitsAsByteArray
        assertEquals(expectedHighBits.size(), compressedList.highBitsAsByteArray.size());
        for (int i = 0; i < expectedHighBits.size(); i++) {
            assertArrayEquals(expectedHighBits.get(i), compressedList.highBitsAsByteArray.get(i));
        }
        // Compare lowBitsAsByteArray
        assertEquals(expectedLowBits.size(), compressedList.lowBitsAsByteArray.size());
        for (int i = 0; i < expectedLowBits.size(); i++) {
            assertArrayEquals(expectedLowBits.get(i), compressedList.lowBitsAsByteArray.get(i));
        }
    }

    @Test
    void testCompress2Elements() {
        // Prepare test data
        List<Posting> postingList = Arrays.asList(
                new Posting(1, 3),
                new Posting(2, 2)
        );
        // Compress the posting list
        assertDoesNotThrow(() ->EliasFano.compress(postingList));
    }

    @Test
    void testCompress1Elements(){
        // Prepare test data
        List<Posting> postingList = List.of(
                new Posting(1, 3)
        );
        // Compress the posting list
        assertDoesNotThrow(() ->EliasFano.compress(postingList));
    }

    @Test
    void testDecompressFirstElementsFromDisk() throws IOException {
        // Prepare test data
        List<Posting> postingList = Arrays.asList(
                new Posting(1, 3),
                new Posting(2, 2),
                new Posting(3, 4),
                new Posting(4, 5),
                new Posting(5, 10)
        );

        // Compress the posting list
        EliasFanoCompressedList compressedList = EliasFano.compress(postingList);

        // Ensure the file is empty before reading
        File file = new File("data/test/CompressedList.dat");
        if (file.exists()) {
            if(!file.delete()){
                throw new RuntimeException();
            }
        }

        // Decompress the posting list
        StreamHelper.createDir("data/test/");
        compressedList.writeToDisk(new FileOutputStream("data/test/CompressedList.dat"));
        List<Integer> decompressedList = EliasFano.decompress(new FileInputStream("data/test/CompressedList.dat"), 0);

        // Assertions
        assertEquals(postingList.size(), decompressedList.size());
        for (int i = 0; i < postingList.size(); i++) {
            assertEquals(postingList.get(i).docid, decompressedList.get(i));
        }
    }

    // execute only this, for some reason if execute all together this test fails; probably is something on concurrency

    @Test
    void testCompressAndDecompressFromDisk() throws IOException {
        // Prepare test data
        List<Posting> postingList = Arrays.asList(
                new Posting(10, 3),
                new Posting(15, 2),
                new Posting(16, 4),
                new Posting(18, 5),
                new Posting(27, 10)
        );

        // Compress the posting list
        EliasFanoCompressedList compressedList = EliasFano.compress(postingList);

        // Ensure the file is empty before reading
        File file = new File("data/test/CompressedList.dat");
        if (file.exists()) {
            if(!file.delete()){
                throw new RuntimeException();
            }
        }

        // Decompress the posting list
        StreamHelper.createDir("data/test/");
        compressedList.writeToDisk(new FileOutputStream("data/test/CompressedList.dat"));
        List<Integer> decompressedList = EliasFano.decompress(new FileInputStream("data/test/CompressedList.dat"), 0);

        // Assertions
        assertEquals(postingList.size(), decompressedList.size());
        for (int i = 0; i < postingList.size(); i++) {
            assertEquals(postingList.get(i).docid, decompressedList.get(i));
        }
    }

}