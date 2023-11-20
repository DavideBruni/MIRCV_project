package unipi.aide.mircv.model;
import org.junit.jupiter.api.Test;
import unipi.aide.mircv.helpers.StreamHelper;

import java.io.*;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EliasFanoTest {



    @Test
    void testDecompressFirstElementsFromDisk() throws IOException {
        // Prepare test data
        List<Posting> postingList = Arrays.asList(
                new Posting(1, 3),
                new Posting(3, 2),
                new Posting(5, 4),
                new Posting(6, 5),
                new Posting(7, 10)
        );

        // Compress the posting list
        EliasFanoCompressedList compressedList = EliasFano.compress(postingList);

        // Decompress the posting list
        StreamHelper.createDir("data/test/");
        compressedList.writeToDisk(new FileOutputStream("data/test/CompressedList.dat"));
        List<Long> decompressedList = EliasFano.decompress(new FileInputStream("data/test/CompressedList.dat"), 0);

        // Assertions
        assertEquals(postingList.size(), decompressedList.size());
        for (int i = 0; i < postingList.size(); i++) {
            assertEquals(postingList.get(i).docid, decompressedList.get(i));
        }
    }


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

        // Decompress the posting list
        StreamHelper.createDir("data/test/");
        compressedList.writeToDisk(new FileOutputStream("data/test/CompressedList.dat"));
        List<Long> decompressedList = EliasFano.decompress(new FileInputStream("data/test/CompressedList.dat"), 0);

        // Assertions
        assertEquals(postingList.size(), decompressedList.size());
        for (int i = 0; i < postingList.size(); i++) {
            assertEquals(postingList.get(i).docid, decompressedList.get(i));
        }
    }

}