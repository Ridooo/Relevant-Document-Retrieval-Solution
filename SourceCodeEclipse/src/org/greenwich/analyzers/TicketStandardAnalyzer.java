package org.greenwich.analyzers;



import java.io.*;
import java.util.*;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.*;

public class TicketStandardAnalyzer extends Analyzer{
    
    private     StandardAnalyzer standardanalyzer;
    private     CharArraySet stopWords ;


    public  TicketStandardAnalyzer(Version version) {
        
    	this.stopWords  = new CharArraySet(Version.LUCENE_30, 30, true);
    	stopWords.add("issue");
    	stopWords.add("helpdesk");
    	stopWords.add(standardanalyzer.STOP_WORDS_SET);
    	
  
         this.standardanalyzer = new StandardAnalyzer(version, this.stopWords); 
       
       
    }
    public TokenStream tokenStream( String fieldName,Reader reader) {
      
        
        TokenStream standardtokenstream = standardanalyzer.tokenStream(fieldName,reader);
        return new PorterStemFilter(standardtokenstream);
      
    }
    
}
