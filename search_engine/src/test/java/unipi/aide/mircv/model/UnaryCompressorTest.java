package unipi.aide.mircv.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class UnaryCompressorTest {

    static Stream<Arguments> parametersGetSize() {
        int [] dimensions = new int []{25,100,1000};
        Arguments [] args = new Arguments[3];
        for(int j = 0; j<dimensions.length ; j++){
            List<Integer> freq = new ArrayList<>();
            for(int i = 1; i<=dimensions[j]; i++){
                freq.add(i);
            }
            int nOfBits = ((dimensions[j] * (dimensions[j]+1))/2) + dimensions[j];
            args[j] = Arguments.of(freq,(int)Math.ceil((double)nOfBits/Byte.SIZE));
        }
        return Stream.of(
                args[0],
                args[1],
                args[2]
        );
    }
    @ParameterizedTest
    @MethodSource("parametersGetSize")
    void getByteSizeInUnary(List<Integer> values, int expected) {
        assertEquals(expected,UnaryCompressor.getByteSizeInUnary(values));
    }

    @Test
    void compress() {
        Integer [] valuesArray = {3,10,1,2};
        List<Integer> values = Arrays.asList(valuesArray);
        byte [] res;
        res = UnaryCompressor.compress(values);
        byte [] expected = new byte[]{-17, -3, 96};
        assertArrayEquals(expected,res);
    }

    @Test
    void decompressFrequencies() {
        Integer [] expectedArray = {3,10,1,2};
        List<Integer> expected = Arrays.asList(expectedArray);
        byte [] compressed = new byte[]{-17, -3, 96};
        List<Integer> res = UnaryCompressor.decompressFrequencies(compressed,4);
        for(Integer x : res){
            assertEquals(x,expected.get(res.indexOf(x)));
        }
    }

    @Test
    void get() {
        Integer [] valuesArray = {3,10,1,2};
        List<Integer> values = Arrays.asList(valuesArray);
        byte [] compress;
        compress = UnaryCompressor.compress(values);
        long[] res = UnaryCompressor.get(compress,1,-1,0);
        assertEquals(10,res[0]);
        res = UnaryCompressor.get(compress,3,1,res[1]);
        assertEquals(2,res[0]);
    }
}