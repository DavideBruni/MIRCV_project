package unipi.aide.mircv.parsing;

import ca.rmen.porterstemmer.PorterStemmer;
import unipi.aide.mircv.exceptions.PidNotFoundException;
import unipi.aide.mircv.log.CustomLogger;
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
    private static List<String> stopwords;

    // This method tokenizes the input text and performs various text processing operations based on the 'parseFlag'.
    public static List<String> getTokens(String text, boolean parseFlag) {
        Pattern pattern = Pattern.compile("[^\\x00-\\x7F]+");       // this regex match all the invalid UTF-8 char
        text = text.replaceAll("<[^>]+>", " "); // Remove HTML
        String[] punctuationMarks = {"!", "\"", "#", "$", "%", "&", "'", "(", ")", "*", "+", ",", "-", ".", "/", ":", ";", "<", "=", ">", "?", "@", "[", "\\", "]", "^", "_", "`", "{", "|", "}", "~"};
        for (String c : punctuationMarks) {
            text = text.replace(c, " ");        //remove punctuation marks
        }
        String[] words = text.toLowerCase().split(" ");     //brings all tokens to lower case
        // remove words containing invalid char (Perchè non rimpiazzarlo? In un documento, non tutte le parole hanno char non validi)
        // remove extra spaces
        // remove words containing invalid UTF-8 chars
        List<String> tokens = new ArrayList<>();

        for (String word : words) {
            // url are not considered and too long words (probably errors are not considered)
            if (!word.isEmpty() && word.length() < 46 && (!word.startsWith("http") || !word.startsWith("www"))) {
                Matcher matcher = pattern.matcher(word);
                if(matcher.find()){
                    continue;
                }
                word = removeConsecutiveCharacter(word.trim());     // remove extra spaces and more than two consecutive not digit chars
                if(word.length() >= 2)
                    tokens.add(word);
            }
        }

        if (parseFlag)
                tokens = stemming(stopwords_filtering(tokens));     //first apply stopwords filtering, then stemming on the remaining ones
        return tokens;
    }

    private static String removeConsecutiveCharacter(String token) {
        if (token == null || token.length() <= 2) {
            return token;
        }

        StringBuilder result = new StringBuilder();
        char prevChar = token.charAt(0);
        result.append(prevChar);
        char currentChar;
        int consecutiveCount = 1;

        for (int j = 1; j < token.length(); j++) {
            currentChar = token.charAt(j);

            if (Character.isDigit(currentChar) == Character.isDigit(prevChar) && currentChar == prevChar) {
                consecutiveCount++;
                if (consecutiveCount <= 2) {
                    result.append(currentChar);
                }
            } else {
                consecutiveCount = 1;
                result.append(currentChar);
            }

            prevChar = currentChar;
        }
        return result.toString();
    }

    // This method performs stemming on a list of tokens using PorterStemmer.
     static List<String> stemming(List<String>  tokens) {
        //customLogger.info("Performing stemming");
        PorterStemmer porterStemmer = new PorterStemmer();
        List<String> stemmedTokens = new ArrayList<>();
        for(String token:tokens){
            stemmedTokens.add(porterStemmer.stemWord(token));
        }
        return stemmedTokens;
    }

    // This method filters out stopwords from a list of tokens.
    static List<String> stopwords_filtering(List<String> tokens) {
        //customLogger.info("Performing stopwords filtering");
        // moveOut
        if(stopwords == null){
            try (Stream<String> lines = Files.lines(Paths.get(STOPWORDS_STRING_PATH))) {
                stopwords = lines.collect(Collectors.toList());
            } catch (IOException e) {
                CustomLogger.error("Unable to perform stopwords filtering: file with stopwords not found");
                return tokens;
            }
        }

        return tokens.stream()
                .filter(token -> !stopwords.contains(token))
                .collect(Collectors.toList());
    }

    public static ParsedDocument parseDocument(String text, boolean parseFlag) throws PidNotFoundException {
        String regex = "\\d+\\t";       // regex to find a sequence of number followed by a tab (our pid)

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(text);
        String pid;

        if (matcher.find()) {
            pid = matcher.group(0).replace("\t", "");       // get the PID
        }else{
            throw new PidNotFoundException();                               // if the regex doesn't match, throw the exception
        }

        text = text.substring(matcher.end());

        // function getTokens: if needed perform stemming and stopwrods filtering
        return new ParsedDocument(pid,getTokens(text,parseFlag));           // return a class containing pid and the tokens
    }
}
