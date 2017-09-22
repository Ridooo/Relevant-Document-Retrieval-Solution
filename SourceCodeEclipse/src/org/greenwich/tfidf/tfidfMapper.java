package org.greenwich.tfidf;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.*;


public class tfidfMapper extends Mapper<Text, Text, Text, Text>{

	
	
 public void map(Text key, Text value, Context context)throws IOException,  InterruptedException  {
		

	 try {
	
		 //calculate collection Frequency for a term
		 JSONObject PostingLists = new JSONObject(value.toString());
		 JSONArray arrayPostingLists = PostingLists.getJSONArray("PostingLists");
		 long colFreq = 0;
		 for ( int i=0;  i <arrayPostingLists.length(); i++) {
			 
			 JSONObject DocObj = arrayPostingLists.getJSONObject(i);
			 colFreq += DocObj.getInt("termFreq");
			 
			 
		 }
		 
		 
		 
		 
		 // create JSON Object that contains the following term characterstic per term per document {"term":term1,"colFreq":4,"DocFreq":2,"termFreq":2} a
		 
		 for ( int i=0;  i <arrayPostingLists.length(); i++) {
			 
			 JSONObject obj = arrayPostingLists.getJSONObject(i);
			 			 
			 JSONObject objValue = new JSONObject();
			 objValue.put("term", key.toString());
			 objValue.put("colFreq", colFreq);
			 objValue.put("DocFreq", PostingLists.getInt("DocFreq"));
			 objValue.put("termFreq", obj.getInt("termFreq"));
			 
			//count term Significancy  in a document throught its location, terms appears in the ticket's summary are given 3, description 2 and comments 1
			 JSONArray TermPositionsArray = obj.getJSONArray("TermPositions");
			 int termLocationCount=0; 
			 for(int j=0; j<TermPositionsArray.length(); j++) {
				 JSONObject TermPosition =TermPositionsArray.getJSONObject(j);
				 termLocationCount +=  TermPosition.getInt("termLocation");
			 }
			 objValue.put("termLocationCount", termLocationCount);
			 
			 context.write(new Text(obj.getString("docID")), new Text(objValue.toString()));
			 
	 } 
	}catch (JSONException e) {
		System.err.println("Caught IOException: " + e.getMessage());
	}
	 
 }



}



