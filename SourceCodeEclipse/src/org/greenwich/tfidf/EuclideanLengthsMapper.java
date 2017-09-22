package org.greenwich.tfidf;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.*;

public class EuclideanLengthsMapper extends Mapper<Text, Text, Text, Text>{

//Key		Value
//10098   [{"term":"dear","termWeight":0},{"term":"delivered","termWeight":0.9030899869919435},{"term":"dispatched","termWeight":0.4259687452152651}]
	 public void map(Text key, Text value, Context context)throws IOException,  InterruptedException  {
		
		 //
	try{
		JSONObject docVector = new JSONObject(value.toString());
		Iterator<String> terms =  docVector.keys();
		Float docNormalizationFactor= new Float(0);
		while (terms.hasNext()) {
				
				Float termWeight =(float)  docVector.getDouble(terms.next());
			 docNormalizationFactor += (float) Math.pow(termWeight, 2);
		}
		
		 
		 
		 docNormalizationFactor= (float) Math.sqrt(docNormalizationFactor);
		 context.write(key, new Text(docNormalizationFactor.toString()));
	
	}catch(JSONException e) {
		System.err.println(e);
	}
	}
	
	 
	 
}
