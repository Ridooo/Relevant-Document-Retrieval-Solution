package org.greenwich.searchEngine;
import java.util.*;
public class ReturnedDocument implements Comparator<ReturnedDocument>{
	
	private String docID;
	private Float docScore;
	
	public ReturnedDocument() {
		
	}
	public ReturnedDocument(String docID, Float docScore) {
		this.docID = docID;
		this.docScore = docScore; 
		
	}
	
	public void setDocID(String docID){
		this.docID = docID;
	}
	public void setDocScore(Float docScore){
		this.docScore = docScore;
	}
	public String getDocID(){
		return docID;
	}
	public Float getDocScore(){
		return docScore;
	}
	public String toString(){
		return docID.toString() +":" + docScore.toString();
	}
	public int compare(ReturnedDocument returnedDocument1,ReturnedDocument returnedDocument2){
		
		int cmp = returnedDocument1.docScore.compareTo(returnedDocument2.docScore);
		return cmp*-1;
		
	}
	
	
}
