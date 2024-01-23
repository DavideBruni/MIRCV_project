package unipi.aide.mircv.model;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.*;

class CompressedPostingListTest {

    /* DISCLAIMER: All other methods have been tested using a collection smaller than collection.tar.gz*/

    static Stream<Arguments> parametersConstructorTest() {
        int [] dimensions = new int []{10,100,1000,10000};
        Arguments [] args = new Arguments[4];
        for(int j = 0; j<dimensions.length ; j++){
            List<Integer> docIds = new ArrayList<>();
            List<Integer> freq = new ArrayList<>();
            for(int i = 1; i<=dimensions[j]; i++){
                docIds.add(i);
                freq.add(i);
            }
            args[j] = Arguments.of(docIds,freq);
        }
        return Stream.of(
                args[0],
                args[1],
                args[2],
                args[3]
        );
    }
    @ParameterizedTest
    @MethodSource("parametersConstructorTest")
    void CompressedPostingListConstructor(List<Integer> docIds, List<Integer> freq) {
        UncompressedPostingList postingList = new UncompressedPostingList(docIds,freq);
        CompressedPostingList compressedPostingList = new CompressedPostingList(postingList);
        compressedPostingList.blockDescriptor.setIndexes(EliasFano.getCompressedSize(postingList.getMaxDocId(),postingList.docIds.size()),UnaryCompressor.getByteSizeInUnary(freq));
        for(int i=0; i<compressedPostingList.blockDescriptor.getNumberOfPostings(); i++){
            int docId = compressedPostingList.docId();
            assertEquals(i+1,docId);
            compressedPostingList.next();
        }
    }

    @ParameterizedTest
    @MethodSource("parametersConstructorTest")
    void nextGEQTest(List<Integer> docIds, List<Integer> freq) {
        UncompressedPostingList postingList = new UncompressedPostingList(docIds,freq);
        CompressedPostingList compressedPostingList = new CompressedPostingList(postingList);
        compressedPostingList.blockDescriptor.setIndexes(EliasFano.getCompressedSize(postingList.getMaxDocId(),postingList.docIds.size()),UnaryCompressor.getByteSizeInUnary(freq));
        for(int i=0; i<compressedPostingList.blockDescriptor.getNumberOfPostings(); i += 5){
            compressedPostingList.nextGEQ(i+5);
            int docId = compressedPostingList.docId();
            assertEquals(i+5,docId);

        }
    }
}