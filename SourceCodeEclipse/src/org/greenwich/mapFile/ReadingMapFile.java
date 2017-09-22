package org.greenwich.mapFile;

import java.net.URI;
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.json.*;
public class ReadingMapFile {

	private String mapUri;
	
	private Configuration conf;
	private FileSystem filesystem;
	private MapFile.Reader reader =null ;
	
	
	public ReadingMapFile(String mapUri)throws IOException{
	
		this.conf = new Configuration();
		  this.mapUri= mapUri;
	    this.filesystem = FileSystem.get(URI.create(mapUri), conf);
	
	}
	
	public JSONObject getTermPostingsList(String term) throws IOException, JSONException{
	   
		Text key ;
		Text value ;
			
		try {
			reader = new MapFile.Reader(filesystem, mapUri, conf);
				key =  new Text(term);
				value = new Text();
				
		if (reader.get(key, value) == null){
			return new JSONObject();
		}
		else {
			reader.get(key, value);
			return new JSONObject(value.toString());
		}
				
			}
		finally {
			IOUtils.closeStream(reader);
		}
	
	
		
	}
	
	public Map<String, Float> getDocEuclideanLengths(Set<String> DocsSet)throws IOException{
		
		
		Map<String, Float> DocsEuclideanLengthsMap = new TreeMap<String, Float> ();
		Text key ;
		Text value ;
			
		try {
			reader = new MapFile.Reader(filesystem, mapUri, conf);
			for(String docID:DocsSet) {
				key =  new Text(docID);
				value = new Text();
				reader.get(key, value);
				DocsEuclideanLengthsMap.put(docID, Float.parseFloat(value.toString()));
				}
			return DocsEuclideanLengthsMap;
			
		}finally {
			IOUtils.closeStream(reader);
		}
	
		
	}
	
	public JSONObject getDocVector(String DocID) throws IOException, JSONException{
		   
		Text key ;
		Text value ;
			
		try {
			reader = new MapFile.Reader(filesystem, mapUri, conf);
				key =  new Text(DocID);
				value = new Text();
				
		if (reader.get(key, value) == null){
			return new JSONObject();
		}
		else {
			reader.get(key, value);
			return new JSONObject(value.toString());
		}
				
			}
		finally {
			IOUtils.closeStream(reader);
		}
	
	
		
	}
	
	
}
