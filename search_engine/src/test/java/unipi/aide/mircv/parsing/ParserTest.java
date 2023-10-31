package unipi.aide.mircv.parsing;

import org.junit.Test;
import unipi.aide.mircv.exceptions.PidNotFoundException;
import unipi.aide.mircv.model.ParsedDocument;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

public class ParserTest {

    @Test
    public void testParseDocumentWithoutStemmingAndStopwordsFiltering() {
        String inputText = "1234\tThis is a sample document for testing. <html> Some <b>HTML</b> tags here!</html>";

        try {
            ParsedDocument parsedDocument = Parser.parseDocument(inputText, false);
            assertEquals("1234", parsedDocument.getPid());
            // Add assertions for tokens without stemming and stopwords filtering
            assertArrayEquals(new String[]{"this", "is", "a", "sample", "document", "for", "testing", "some", "html", "tags", "here"}, parsedDocument.getTokens().toArray());
        } catch (PidNotFoundException e) {
            fail("PID not found in the input text.");
        }
    }

    @Test
    public void testParseDocumentWithStemmingAndStopwordsFiltering() {
        String inputText = "1234\tThis is a sample document for testing. <html> Some <b>HTML</b> tags here!</html>";

        try {
            ParsedDocument parsedDocument = Parser.parseDocument(inputText, true);
            assertEquals("1234", parsedDocument.getPid());
            // Add assertions for tokens with stemming and stopwords filtering
            assertArrayEquals(new String[]{"sampl", "document", "test", "html", "tag"}, parsedDocument.getTokens().toArray());
        } catch (PidNotFoundException e) {
            fail("PID not found in the input text.");
        }
    }

    @Test
    public void testPidNotFoundException() {
        String inputText = "This text does not contain a PID.";

        assertThrows(PidNotFoundException.class, () -> {
            Parser.parseDocument(inputText, true);
        });
    }
}
