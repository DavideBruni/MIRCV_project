package unipi.aide.mircv.parsing;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class ParserTest {


    @Test
    public void testStopwordsFiltering() {
        // Mock input data
        List<String> tokens = Arrays.asList("this", "is", "a", "test", "with", "some", "stopwords");

        // Test stopwords filtering
        List<String> result = Parser.stopwords_filtering(tokens);

        // Define expected output after stopwords filtering
        List<String> expected = Arrays.asList("test", "stopwords");

        // Assert that the result matches the expected output
        assertEquals(expected, result);
    }

    @Test
    public void testStemming() {
        // Mock input data
        List<String> tokens = Arrays.asList("running", "jumps", "quickly");

        // Test stemming
        List<String> result = Parser.stemming(tokens);

        // Define expected output after stemming
        List<String> expected = Arrays.asList("run", "jump", "quickli");

        // Assert that the result matches the expected output
        assertEquals(expected, result);
    }

    @Test
    public void testRemoveInvalidCharacters() {
        // Mock input data
        List<String> tokens = Arrays.asList("valid", "in", "vàlid", "unicode", "character");

        // Test removeInvalidCharacters
        List<String> result = Parser.removeInvalidCharacters(tokens);

        // Define expected output after removing invalid characters
        List<String> expected = Arrays.asList("valid", "in", "vlid", "unicode", "character");

        // Assert that the result matches the expected output
        assertEquals(expected, result);
    }

    @Test
    public void testGetTokens() {
        // Mock input data
        String text = "<html>This is a test! It contains <b>HTML</b> tags.</html>";

        // Test getTokens
        List<String> result = Parser.getTokens(text, true);

        // Define expected output after tokenization and processing
        List<String> expected = Arrays.asList("test", "contain", "html", "tag");

        // Assert that the result matches the expected output
        assertEquals(expected, result);
    }

}
