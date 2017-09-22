package org.greenwich.mapFile;

import java.io.IOException;
import java.net.URI;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.MapFile;

import org.apache.hadoop.io.SequenceFile;

public class MapFileFixer {
	String mapUri ;
	Configuration conf ;
	FileSystem fs;
	Path map ;
	Path mapData ;
	
	public MapFileFixer() {
		
	}
	
	public void convertSeqToMapFile(Configuration conf,String mapUri) throws IOException, Exception{
		this.mapUri = mapUri;
		this.conf =conf;

			
		this.fs = FileSystem.get(URI.create(mapUri), conf);
		this.map = new Path(mapUri+"/part-r-00000");
		
		
		
		SequenceFile.Reader reader = null;
		MapFile.Writer writer = null;
		try {
			reader = new SequenceFile.Reader(fs, map, conf);
			WritableComparable key = (WritableComparable) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(),conf);
			writer = new MapFile.Writer(conf, fs,this.mapUri, key.getClass(),value.getClass());
			writer.setIndexInterval(1);
			while (reader.next(key, value)) {
				
				writer.append(key, value);
				
				
			}
		}
		finally {
			IOUtils.closeStream(reader);
			IOUtils.closeStream(writer);
			
		}
		
	}
	
	
	
	

}
