package org.greenwich.searchEngine;

import java.io.*;
import java.util.*;
import java.text.*;

import org.greenwich.analyzers.*;
import org.greenwich.invertedIndex.TermWritable;
import org.json.*;




public class SearchEngine {
	


		
	public static void main (String[] args) throws IOException, JSONException{
			
		String query = args[0];
		Date date = new Date();
		System.out.println(date.toString());
		
		
	long N = 19542;  // number of documents in the collection
	QueryAnalyzer queryAnalyzer = new QueryAnalyzer(query);

	List<TermWritable> QueryTermsList = queryAnalyzer.analyzeQuery();
	
	Ranking ranking = new Ranking(N);
	// List<ReturnedDocument> returnedDocument = ranking.calCosineScore(QueryTermsList);
	 List<ReturnedDocument> returnedDocument = ranking.calCosineScore(QueryTermsList);
	
	if (returnedDocument.size() == 0){
		System.out.println("No matching issues found.");
		
	}
	else {
		System.out.println(returnedDocument.size());
		for (ReturnedDocument x:returnedDocument.subList(0,5))
		{
			System.out.println(x);
				
		}
		
	}
	           
	Date date1 = new Date();
	System.out.println(date1.toString());
	
		}

	


}
