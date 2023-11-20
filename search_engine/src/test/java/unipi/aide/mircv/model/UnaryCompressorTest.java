package unipi.aide.mircv.model;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UnaryCompressorTest {

    @Test
    void testCompressWithClusters() {
        int[] clusters = {3, 0, 2, 1};

        List<BitSet> compressedList = UnaryCompressor.compress(clusters);

        assertEquals(4, compressedList.size());

        BitSet expectedCluster0 = new BitSet(4);
        expectedCluster0.set(1, 4); // 0b1110
        BitSet expectedCluster1 = new BitSet(1);
        BitSet expectedCluster2 = new BitSet(3);
        expectedCluster2.set(1, 3); // 0b110
        BitSet expectedCluster3 = new BitSet(2);
        expectedCluster3.set(1, 2); // 0b10

        assertEquals(expectedCluster0, compressedList.get(0));
        assertEquals(expectedCluster1, compressedList.get(1));
        assertEquals(expectedCluster2, compressedList.get(2));
        assertEquals(expectedCluster3, compressedList.get(3));
    }

    @Test
    void testCompressWithPostingList() {
        List<Posting> postingList = new ArrayList<>();
        postingList.add(new Posting(2, 3));
        postingList.add(new Posting(4, 1));
        postingList.add(new Posting(6, 5));

        List<BitSet> compressedList = UnaryCompressor.compress(postingList);

        assertEquals(3, compressedList.size());

        BitSet expectedPosting0 = new BitSet(1);
        expectedPosting0.set(1, 4); // 0b1110
        BitSet expectedPosting1 = new BitSet(2);
        expectedPosting1.set(1, 2); // 0b10
        BitSet expectedPosting2 = new BitSet(3);
        expectedPosting2.set(1, 6); // 0b111110

        assertEquals(expectedPosting0, compressedList.get(0));
        assertEquals(expectedPosting1, compressedList.get(1));
        assertEquals(expectedPosting2, compressedList.get(2));
    }
}
