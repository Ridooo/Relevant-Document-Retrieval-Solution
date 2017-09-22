package org.greenwich.analyzers;





import java.io.StringReader;
import java.util.*;
import java.util.List;
import java.io.*;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.util.Version;
import org.greenwich.invertedIndex.TermWritable;


@SuppressWarnings("deprecation")
public class QueryAnalyzer {
	
	private String query;
	
	public QueryAnalyzer(String query) {
		this.query = query;
	}

	
	public List<TermWritable> analyzeQuery() throws IOException {
		List<TermWritable> queryTerms = new LinkedList<TermWritable>();
		TicketStandardAnalyzer ticketstandardanalyzer = new TicketStandardAnalyzer(Version.LUCENE_30);
		TokenStream standardtokenstream = ticketstandardanalyzer.tokenStream("contents", new StringReader(query.toString()));
		TermAttribute term = standardtokenstream.addAttribute(TermAttribute.class);


        while (standardtokenstream.incrementToken()) {
			TermWritable termWritable = new TermWritable();
			termWritable.setTerm(term.term());
			queryTerms.add(termWritable);
			         }
        ticketstandardanalyzer.close();
        return queryTerms;
		
	}
	
}
