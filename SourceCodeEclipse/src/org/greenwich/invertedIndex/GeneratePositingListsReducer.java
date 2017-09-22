
package org.greenwich.invertedIndex;

import java.io.IOException;
import java.util.*;

import org.json.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GeneratePositingListsReducer extends Reducer<Text,TermWritable,Text,Text>{
	


		
		public void reduce(Text key, Iterable<TermWritable> values, Context context) throws IOException, InterruptedException {
			
			try{
			   
			TreeMap<String,JSONArray> DocPosMap = new TreeMap<String,JSONArray>();
	        
			for(TermWritable value: values) {
	    		
	            TermWritable termWritable = value;
	            String [] docIDPos = new String[6];
	            docIDPos[0] = termWritable.getDocID().toString();
	            docIDPos[1] = termWritable.getPosition().toString();
	            docIDPos[2] = termWritable.getStartOffset().toString();
	            docIDPos[3] = termWritable.getEndtOffset().toString();
	            docIDPos[4] = termWritable.getType().toString();
	            docIDPos[5] = termWritable.getLocation().toString();
	      
	            JSONObject obj= new JSONObject();
	            obj.put("position",docIDPos[1]);
	            obj.put("startOffset",docIDPos[2]);
	            obj.put("endOffset",docIDPos[3]);
	            obj.put("termType",docIDPos[4]);
	            obj.put("termLocation",docIDPos[5]);
	            
	            
	          
	       if (DocPosMap.containsKey(docIDPos[0])){
		
	            	JSONArray array = (JSONArray)DocPosMap.get(docIDPos[0]);
	            	array.put(obj);
						
			DocPosMap.put(docIDPos[0],array);
	            }
						
			else {
				JSONArray array = new JSONArray();
				
				array.put(obj);
				DocPosMap.put(docIDPos[0],array);
			}
			}
	    
	        	
					
				
				JSONArray PostingListsArray = new JSONArray();
				
			Set<Map.Entry<String, JSONArray>> setDocPosMap = DocPosMap.entrySet();
			Iterator<Map.Entry<String, JSONArray>> IT = setDocPosMap.iterator();
			
			while (IT.hasNext()) {
				
				
				Map.Entry<String, JSONArray> ME = (Map.Entry<String, JSONArray>) IT.next();
				
				
				JSONObject obj= new JSONObject();
				obj.put("docID", ME.getKey());
				obj.put("termFreq", ME.getValue().length());
				obj.put("TermPositions", ME.getValue());
				
				
				PostingListsArray.put(obj);
								
			}
			
			JSONObject obj= new JSONObject();
			obj.put("DocFreq", PostingListsArray.length());
			obj.put("PostingLists", PostingListsArray);
					
			context.write(key, new Text(obj.toString()));
			}
			catch (JSONException e){
				
			}
			
			
			
			
		}

	}



