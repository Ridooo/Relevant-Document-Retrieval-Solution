package org.greenwich.searchEngine;
import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapFile;
import org.greenwich.invertedIndex.TermWritable;
import org.greenwich.mapFile.ReadingMapFile;
import org.greenwich.termWeighting.TermWeighting;
import org.json.*;




public class Ranking {

	public long N;
	private String mapUriInvertedIndex = "/data/InvertedIndexOutFolder/";
	private String mapUriEuclideanLengths = "/data/EuclideanLengthsOutFolder/";
	private String mapUriTFIDF = "/data/tfidf2OutFolder/";
	private ReadingMapFile readingMapFileInvertedIndex ;
	private ReadingMapFile readingMapFileEuclideanLengths ;
	private ReadingMapFile readingMapFileTFIDF;
	
	public Ranking(long N) throws IOException{
		this.N = N;
	
		}
	
	
	public List<ReturnedDocument> calCosineScore(List<TermWritable> QueryTermsList)throws IOException, JSONException {
		
		Map<String,Float> DocsScoresMap = new TreeMap<String,Float>();
		
		// calculate the term frequencies for query terms
		Map<String,Integer> termFreqsMap = calcTermQeuryFreqs(QueryTermsList);
	
		
		TermWeighting termWeighting = new TermWeighting();
		
		
		Float queryNormalizationFactor= new Float(0);
		readingMapFileInvertedIndex =  new ReadingMapFile(mapUriInvertedIndex);
		for(TermWritable queyTerm:QueryTermsList){
			
			//calculate term query weighting
			String Term = queyTerm.getTerm().toString();
			Integer termQueryFreq = termFreqsMap.get(Term);
			JSONObject TermPostingList;
			if ( readingMapFileInvertedIndex.getTermPostingsList(Term).length() != 0)
			{
				 TermPostingList = readingMapFileInvertedIndex.getTermPostingsList(Term);

					long termDocFreq = getTermDocFreq(TermPostingList);
					
					Float TermQueryWeighting = termWeighting.calTermQueryWeight(N, termQueryFreq, termDocFreq);
					queryNormalizationFactor +=  (float) Math.pow(TermQueryWeighting, 2);
					
					JSONArray TermPostingListArray = TermPostingList.getJSONArray("PostingLists");
					for(int i=0; i <TermPostingListArray.length(); i++){
						 JSONObject Posting = TermPostingListArray.getJSONObject(i);
						 String docID = Posting.getString("docID");
						 Integer termFreq = new Integer(Posting.getInt("termFreq"));
						 JSONArray TermPositionsArray = Posting.getJSONArray("TermPositions");
						 int termLocationCount=0; 
						 for(int j=0; j<TermPositionsArray.length(); j++) {
							 JSONObject TermPosition =TermPositionsArray.getJSONObject(j);
							 termLocationCount +=  TermPosition.getInt("termLocation");
						 }
						
						 Float TermDocWeighting = new TermWeighting().calTermDocWeight(N, termFreq, termDocFreq, termLocationCount);
						 if (DocsScoresMap.containsKey(docID)){
							 DocsScoresMap.put(docID,DocsScoresMap.get(docID) + (TermDocWeighting*TermQueryWeighting));
						 }
						 else {
							 DocsScoresMap.put(docID,TermDocWeighting*TermQueryWeighting);
								
						 }
						 
						
					}
			}
			else {
				continue;
			}
			

		}
		
		if  ( DocsScoresMap.size() == 0){
			List<ReturnedDocument> DocumentsList = new ArrayList(); 
			return DocumentsList;
		}
		
		else {

			//The Map of DocID - Length holds the lengths (normalization factors) for each of document in DocumentsList
			readingMapFileEuclideanLengths = new ReadingMapFile(mapUriEuclideanLengths);
			Map<String, Float> DocsEuclideanLengthsMap = readingMapFileEuclideanLengths.getDocEuclideanLengths(DocsScoresMap.keySet()); 
			
						
			//divide the score of each document by its normalization factor
			for(String docID:DocsScoresMap.keySet()){
				Float score = DocsScoresMap.get(docID);
				score = score / (DocsEuclideanLengthsMap.get(docID)*queryNormalizationFactor);
				DocsScoresMap.put(docID,score );
			}
						
						
			//Sort the documents based on their scores.
			List<ReturnedDocument> DocumentsList = sortDocumentScores(DocsScoresMap);
				
				return DocumentsList;
			
		}
		
	}
	public List<ReturnedDocument> calCosineScoreTFIDF(List<TermWritable> QueryTermsList)throws IOException, JSONException {
		
		Map<String,Float> DocsScoresMap = new TreeMap<String,Float>();
		
		// calculate the term frequencies for query terms
		Map<String,Integer> termFreqsMap = calcTermQeuryFreqs(QueryTermsList);
	
		
		TermWeighting termWeighting = new TermWeighting();
		
		
		Float queryNormalizationFactor= new Float(0);
		readingMapFileInvertedIndex =  new ReadingMapFile(mapUriInvertedIndex);
		readingMapFileTFIDF		    =  new ReadingMapFile(mapUriTFIDF);
		Map<String, JSONObject> TFIDFMap = new TreeMap<String, JSONObject>();
		for(TermWritable queyTerm:QueryTermsList){
			
			//calculate term query weighting
			String Term = queyTerm.getTerm().toString();
			Integer termQueryFreq = termFreqsMap.get(Term);
			JSONObject TermPostingList;
			if ( readingMapFileInvertedIndex.getTermPostingsList(Term).length() != 0)
			{
				 TermPostingList = readingMapFileInvertedIndex.getTermPostingsList(Term);

					long termDocFreq = getTermDocFreq(TermPostingList);
					
					Float TermQueryWeighting = termWeighting.calTermQueryWeight(N, termQueryFreq, termDocFreq);
					queryNormalizationFactor +=  (float) Math.pow(TermQueryWeighting, 2);
					
					JSONArray TermPostingListArray = TermPostingList.getJSONArray("PostingLists");
					for(int i=0; i <TermPostingListArray.length(); i++){
						 JSONObject Posting = TermPostingListArray.getJSONObject(i);
						 String docID = Posting.getString("docID");
						 Float TermDocWeighting;
						 if (TFIDFMap.containsKey(docID) ) {
							 TermDocWeighting = (float)  TFIDFMap.get(docID).getDouble(queyTerm.getTerm().toString());
							 
						 }
						 else {
							  TFIDFMap.put(docID, readingMapFileTFIDF.getDocVector(docID));
							  TermDocWeighting = (float)  TFIDFMap.get(docID).getDouble(queyTerm.getTerm().toString());
						 }
						 
						 
						
						 
						 
						 if (DocsScoresMap.containsKey(docID)){
							 DocsScoresMap.put(docID,DocsScoresMap.get(docID) + (TermDocWeighting*TermQueryWeighting));
						 }
						 else {
							 DocsScoresMap.put(docID,TermDocWeighting*TermQueryWeighting);
								
						 }
						 
						
					}
			}
			else {
				continue;
			}
			

		}
		
		if  ( DocsScoresMap.size() == 0){
			List<ReturnedDocument> DocumentsList = new ArrayList(); 
			return DocumentsList;
		}
		
		else {

			//The Map of DocID - Length holds the lengths (normalization factors) for each of document in DocumentsList
			readingMapFileEuclideanLengths = new ReadingMapFile(mapUriEuclideanLengths);
			Map<String, Float> DocsEuclideanLengthsMap = readingMapFileEuclideanLengths.getDocEuclideanLengths(DocsScoresMap.keySet()); 
			
						
			//divide the score of each document by its normalization factor
			for(String docID:DocsScoresMap.keySet()){
				Float score = DocsScoresMap.get(docID);
				score = score / (DocsEuclideanLengthsMap.get(docID)*queryNormalizationFactor);
				DocsScoresMap.put(docID,score );
			}
						
						
			//Sort the documents based on their scores.
			List<ReturnedDocument> DocumentsList = sortDocumentScores(DocsScoresMap);
				
				return DocumentsList;
			
		}
		
	}	

	public long getTermDocFreq(JSONObject TermPostingList) throws JSONException{
		
		long TermDocFreq = (long)TermPostingList.getLong("DocFreq");
		
		return TermDocFreq;
	}
	
	public Map<String,Integer> calcTermQeuryFreqs(List<TermWritable> QueryTermsList) {
		
		Map<String,Integer> termFreqsMap = new TreeMap<String,Integer>();
	
		for(TermWritable queyTerm:QueryTermsList){
			
			if(termFreqsMap.containsKey(queyTerm.getTerm().toString())){
				Integer termFreq = termFreqsMap.get(queyTerm.getTerm().toString());
				termFreq++;
				termFreqsMap.put(queyTerm.getTerm().toString(), termFreq);
			}
			else{
			termFreqsMap.put(queyTerm.getTerm().toString(), new Integer(1));
			}
		}
	
		return termFreqsMap;
		
	}
	
	public List<ReturnedDocument> sortDocumentScores(Map<String, Float> DocsScoresMap ){
		List<ReturnedDocument> DocumentsList = new ArrayList<ReturnedDocument>();
		for(String docID:DocsScoresMap.keySet()){
			ReturnedDocument returnedDocument = new ReturnedDocument();
			returnedDocument.setDocID(docID);
			returnedDocument.setDocScore(DocsScoresMap.get(docID));
			DocumentsList.add(returnedDocument);
		}
		
		
		
		 Collections.sort(DocumentsList, new ReturnedDocument());

		 return DocumentsList;
	}


	
	
}
