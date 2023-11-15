package unipi.aide.mircv.parsing;

import ca.rmen.porterstemmer.PorterStemmer;
import unipi.aide.mircv.exceptions.PidNotFoundException;
import unipi.aide.mircv.model.ParsedDocument;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Parser {

    private static final String STOPWORDS_STRING_PATH = "utils/stopwords.txt";

    // This method tokenizes the input text and performs various text processing operations based on the 'parseFlag'.
    public static List<String> getTokens(String text, boolean parseFlag) {
        text = text.replaceAll("<[^>]+>", " "); // Remove HTML
        String[] punctuationMarks = {"!", "\"", "#", "$", "%", "&", "'", "(", ")", "*", "+", ",", "-", ".", "/", ":", ";", "<", "=", ">", "?", "@", "[", "\\", "]", "^", "_", "`", "{", "|", "}", "~"};
        for (String c : punctuationMarks) {
            text = text.replace(c, " ");        //remove punctuation marks
        }
        String[] words = text.toLowerCase().split(" ");     //brings all tokens to lower case
        List<String> tokens = new ArrayList<>();
        for (String word : words) {
            if (!word.isEmpty()) {
                tokens.add(word);
            }
        }
        tokens = removeInvalidCharacters(tokens);
        if (parseFlag)
                tokens = stemming(stopwords_filtering(tokens));     //first apply stopwords filtering, then stemming on the remaining ones
        return tokens;
    }

    // This method performs stemming on a list of tokens using PorterStemmer.
    private static List<String> stemming(List<String>  tokens) {
        PorterStemmer porterStemmer = new PorterStemmer();
        List<String> stemmedTokens = new ArrayList<>();
        for(String token:tokens){
            stemmedTokens.add(porterStemmer.stemWord(token));
        }
        return stemmedTokens;
    }

    // This method filters out stopwords from a list of tokens.
    private static List<String> stopwords_filtering(List<String> tokens) {
        List<String> stopwords = new ArrayList<>();
        try (Stream<String> lines = Files.lines(Paths.get(STOPWORDS_STRING_PATH))) {
            stopwords = lines.collect(Collectors.toList());
        } catch (IOException e) {
            // ADD PRINT ERROR ON LOG_ERROR;
        }
        for(int i = 0; i<tokens.size(); ){
            if(stopwords.contains(tokens.get(i)))
                    tokens.remove(tokens.get(i));
            else
                i++;
        }
        return tokens;
    }

    // This method removes invalid UTF-8 characters from a list of tokens.
    private static List<String> removeInvalidCharacters(List<String> tokens) {
        ArrayList<String> cleanedTokens = new ArrayList<>();
        Pattern pattern = Pattern.compile("[^\\x00-\\x7F]+");
        for(String token : tokens){
            Matcher matcher = pattern.matcher(token);
            String cleanedText = matcher.replaceAll("");
            if (cleanedText.length() >= 2)
                cleanedTokens.add(cleanedText);
        }
        return cleanedTokens;
    }

    public static ParsedDocument parseDocument(String text, boolean parseFlag) throws PidNotFoundException {
        String regex = "\\d+\\t";
        Pattern pattern = Pattern.compile(regex);

        Matcher matcher = pattern.matcher(text);

        String pid;

        if (matcher.find()) {
            pid = matcher.group(0).replace("\t", "");
        }else{
            throw new PidNotFoundException();
        }

        text = text.substring(matcher.end());

        return new ParsedDocument(pid,getTokens(text,parseFlag));
    }
}
