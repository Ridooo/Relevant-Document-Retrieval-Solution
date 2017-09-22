package org.greenwich.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.*;

public class tfidfMapper2 extends Mapper<Text, Text, Text, Text>{

	
	
 public void map(Text key, Text value, Context context)throws IOException,  InterruptedException  {
		
	 // Key 		Value
	 //10051	[{"DocFreq":1,"colFreq":1,"term":"tll","termFreq":1},{"DocFreq":8,"colFreq":19,"term":"email","termFreq":2},{"DocFreq":8,"colFreq":17,"term":"ecc","termFreq":1},{"DocFreq":1,"colFreq":1,"term":"done","termFreq":1},{"DocFreq":8,"colFreq":21,"term":"dear","termFreq":1},{"DocFreq":8,"colFreq":19,"term":"team","termFreq":2},{"DocFreq":8,"colFreq":15,"term":"support","termFreq":2},{"DocFreq":3,"colFreq":3,"term":"data","termFreq":1},{"DocFreq":8,"colFreq":18,"term":"customer.pl","termFreq":2},{"DocFreq":6,"colFreq":9,"term":"confirm","termFreq":1},{"DocFreq":1,"colFreq":1,"term":"conducting","termFreq":1},{"DocFreq":4,"colFreq":8,"term":"closed","termFreq":1},{"DocFreq":5,"colFreq":6,"term":"believe","termFreq":1},{"DocFreq":1,"colFreq":1,"term":"stuff","termFreq":1},{"DocFreq":5,"colFreq":7,"term":"bank","termFreq":1},{"DocFreq":2,"colFreq":2,"term":"specific","termFreq":1},{"DocFreq":1,"colFreq":1,"term":"assiged","termFreq":1},{"DocFreq":1,"colFreq":1,"term":"2007041710000019","termFreq":1},{"DocFreq":1,"colFreq":1,"term":"1","termFreq":1},{"DocFreq":1,"colFreq":1,"term":"sheet","termFreq":1},{"DocFreq":1,"colFreq":2,"term":"session","termFreq":2},{"DocFreq":1,"colFreq":1,"term":"result","termFreq":1},{"DocFreq":5,"colFreq":11,"term":"resolved","termFreq":2},{"DocFreq":6,"colFreq":10,"term":"resolution","termFreq":1},{"DocFreq":1,"colFreq":1,"term":"requset","termFreq":1},{"DocFreq":2,"colFreq":3,"term":"reports","termFreq":1},{"DocFreq":1,"colFreq":1,"term":"00","termFreq":1},{"DocFreq":4,"colFreq":6,"term":"report","termFreq":2},{"DocFreq":2,"colFreq":2,"term":"rami","termFreq":1},{"DocFreq":4,"colFreq":6,"term":"yanal","termFreq":1},{"DocFreq":5,"colFreq":7,"term":"progresssoft","termFreq":1},{"DocFreq":6,"colFreq":20,"term":"problem","termFreq":1},{"DocFreq":8,"colFreq":14,"term":"please","termFreq":1},{"DocFreq":2,"colFreq":2,"term":"pages","termFreq":1},{"DocFreq":8,"colFreq":20,"term":"web","termFreq":2},{"DocFreq":1,"colFreq":1,"term":"17/04/2007","termFreq":1},{"DocFreq":2,"colFreq":3,"term":"about","termFreq":1},{"DocFreq":8,"colFreq":18,"term":"194.165.134.179/otrs","termFreq":2},{"DocFreq":4,"colFreq":5,"term":"above","termFreq":1},{"DocFreq":1,"colFreq":2,"term":"0478","termFreq":2},{"DocFreq":2,"colFreq":5,"term":"page","termFreq":1},{"DocFreq":1,"colFreq":1,"term":"orientation","termFreq":1},{"DocFreq":4,"colFreq":7,"term":"now","termFreq":1},{"DocFreq":7,"colFreq":8,"term":"we","termFreq":1},{"DocFreq":1,"colFreq":1,"term":"no.478","termFreq":1},{"DocFreq":3,"colFreq":3,"term":"need","termFreq":1},{"DocFreq":2,"colFreq":4,"term":"mohammad","termFreq":2},{"DocFreq":1,"colFreq":1,"term":"kilani","termFreq":1},{"DocFreq":2,"colFreq":2,"term":"karram","termFreq":1},{"DocFreq":8,"colFreq":24,"term":"issue","termFreq":1},{"DocFreq":2,"colFreq":3,"term":"information","termFreq":1},{"DocFreq":8,"colFreq":18,"term":"http","termFreq":2},{"DocFreq":8,"colFreq":18,"term":"helpdesk@progressoft.com","termFreq":2},{"DocFreq":7,"colFreq":9,"term":"helpdesk","termFreq":1},{"DocFreq":4,"colFreq":6,"term":"haddad","termFreq":1},{"DocFreq":1,"colFreq":1,"term":"alkaram","termFreq":1},{"DocFreq":1,"colFreq":3,"term":"visit","termFreq":3},{"DocFreq":2,"colFreq":3,"term":"generating","termFreq":1},{"DocFreq":6,"colFreq":17,"term":"from","termFreq":1},{"DocFreq":1,"colFreq":3,"term":"form","termFreq":3},{"DocFreq":8,"colFreq":8,"term":"etsm","termFreq":1},{"DocFreq":8,"colFreq":8,"term":"etds","termFreq":1}]
	 	
	 
	 try {
		 
		 JSONArray PostingEntry =new JSONArray(value.toString());
		 JSONArray TermsWeightsArray =new JSONArray();
		 JSONObject TermWeight = new JSONObject();
		 long docTerms = PostingEntry.length(); 		//number of terms in document
		 long N=19542;  									// the total number of documents in a collection
		 
			for (int i= 0; i<PostingEntry.length(); i++)
				{
				String  	term;
				long 	    colFreq; 			// the collection frequency for this term
				long		DocFreq;  			// the number of documents in the collection that contain the term
				long		termFreq;		// number of times the term appears in this document
				long 		termLocationCount;
				float 	tf;
				float 	idf;
				float	termWeight;	
				// a composite weight for each term in each document
				JSONObject Term =PostingEntry.getJSONObject(i);
				
				term= Term.getString("term");
				colFreq= Term.getLong("colFreq");
				termFreq= Term.getLong("termFreq");
				DocFreq= Term.getLong("DocFreq");
				termLocationCount = Term.getLong("termLocationCount");

				tf= (float) Math.log(termFreq+termLocationCount);					// term frequeny
				idf =(float) Math.log10(N+1/DocFreq);					//inverse document	frequency
				termWeight = tf*idf;
				
				
				TermWeight.put(term, termWeight);
			
				}
		
			context.write(key,new Text(TermWeight.toString()));
			
	 }catch (JSONException e){
		 
		 System.err.println("Caught IOException: " + e.getMessage());
		 
	 }
	 
	 		
		
		
 }



}


