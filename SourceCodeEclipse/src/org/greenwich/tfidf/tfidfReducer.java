package org.greenwich.tfidf;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.*;


public class tfidfReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key , Iterable<Text> values,Context context) throws IOException,InterruptedException {
		JSONArray array =new JSONArray();

		try {
		for (Text value:values) {
			
			JSONObject obj = new JSONObject(value.toString());
			array.put(obj);
			
		}
		context.write(key, new Text(array.toString()));
		
	}
		catch (JSONException e) {
		System.err.println("Exception " +e );
	}
	
}
}
