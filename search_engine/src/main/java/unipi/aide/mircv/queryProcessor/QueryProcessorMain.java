package unipi.aide.mircv.queryProcessor;

import unipi.aide.mircv.model.Lexicon;
import unipi.aide.mircv.model.PostingList;
import unipi.aide.mircv.parsing.Parser;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Scanner;

public class QueryProcessorMain {
    public static void main( String[] args ){
        if (args.length < 2 ) {
            // Error in input parameters, write log
            System.exit(-1);
        }

        boolean parse = true;
        boolean compressed = false;

        // Creare uno scanner per leggere da System.in (standard input)
        Scanner scanner = new Scanner(System.in);

        while(true){
            System.out.println("Insert new query\n");

            // Leggere la stringa dalla standard input
            String query = scanner.nextLine();

            List<String> parsedQuery = Parser.getTokens(query,parse);

            PostingList[] postingLists = getPostingLists(parsedQuery);
            PriorityQueue<Scorer.DocScorePair> queryResult = null;
            if (postingLists.length != 0)
                queryResult = Scorer.maxScore(postingLists);

            // printing result
            if(queryResult == null){
                System.out.println("No results found! \n\n");
            }else{
                System.out.println(queryResult + "\n\n");
            }

            Lexicon.clear();

            //exit condition
            if(query.equals('\u0005'))
                break;
        }

        // Chiudere lo scanner dopo l'uso
        scanner.close();

    }

    private static PostingList[] getPostingLists(List<String> terms) {
        List<PostingList> postingLists = new ArrayList<>();
        for(String term : terms){
            postingLists.add(new PostingList(term));
        }
        return (PostingList[]) postingLists.toArray();
    }
}
