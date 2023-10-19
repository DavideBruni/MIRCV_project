package unipi.aide.mircv.parsing;

import unipi.aide.mircv.exceptions.PidNotFoundException;

import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class Parser {


    public static String getPid(String text) throws PidNotFoundException {
        // Define and compile regex
        String regex = "\\d+\\t";
        Pattern pattern = Pattern.compile(regex);

        Matcher matcher = pattern.matcher(text);

        String pid = null;

        if (matcher.find()) {
            pid = matcher.group(0).replace("\t", "");
        }else{
            throw new PidNotFoundException();
        }
        return pid;
    }

    public static List<String> getTokens(String line, boolean parse) {
        return null;
    }
}
