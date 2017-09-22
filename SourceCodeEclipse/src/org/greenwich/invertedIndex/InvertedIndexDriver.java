package org.greenwich.invertedIndex;


import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.greenwich.mapFile.MapFileFixer;
import org.greenwich.tfidf.EuclideanLengthsDriver;
import org.greenwich.tfidf.EuclideanLengthsMapper;
import org.greenwich.tfidf.tfidfMapper;
import org.greenwich.tfidf.tfidfMapper2;
import org.greenwich.tfidf.tfidfReducer;


public class InvertedIndexDriver extends Configured implements Tool{
	

	public String InvertedIndexInFolder ;
	public static String InvertedIndexOutFolder = "/data/InvertedIndexOutFolder";
	public String tfidf1InFolder = InvertedIndexOutFolder;
	public String tfidf1OutFolder = "/data/tfidf1OutFolder";
	public String tfidf2InFolder = tfidf1OutFolder;
	public static String tfidf2OutFolder = "/data/tfidf2OutFolder";
	public String EuclideanLengthsInFolder = tfidf2OutFolder;
	public static String EuclideanLengthsOutFolder = "/data/EuclideanLengthsOutFolder";
	
	public int run(String [] args) throws IOException, InterruptedException , Exception{
		
		if(args.length != 2) {
			System.err.printf("Usage: %s [generic-options] <INPUT> <OUTPUT>", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		InvertedIndexInFolder =  args[0];
		
		
        Configuration conf =getConf();
               
        Path  InvertedIndexOutPath = new Path(InvertedIndexOutFolder);
		FileSystem fs = InvertedIndexOutPath.getFileSystem(conf);
		if (fs.exists(InvertedIndexOutPath)){
			fs.delete(InvertedIndexOutPath,true);
	    }
		
		Path  tfidf1OutPath = new Path(tfidf1OutFolder);
		if (fs.exists(tfidf1OutPath)){
			fs.delete(tfidf1OutPath,true);
	    }
		Path  tfidf2OutPath = new Path(tfidf2OutFolder);
		if (fs.exists(tfidf2OutPath)){
			fs.delete(tfidf2OutPath,true);
	    }
		Path  EuclideanLengthsOutPath = new Path(EuclideanLengthsOutFolder);
		if (fs.exists(EuclideanLengthsOutPath)){
			fs.delete(EuclideanLengthsOutPath,true);
	    }
		
		Job job =new Job(conf,"InvertedIndexJob");
		job.setJarByClass(InvertedIndexDriver.class);
		
		job.setInputFormatClass(TicketFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(GenerateTermsMapper.class);
		job.setReducerClass(GeneratePositingListsReducer.class);
               
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TermWritable.class);
                
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(InvertedIndexInFolder));
		FileOutputFormat.setOutputPath(job, new Path(InvertedIndexOutFolder));
		
		job.waitForCompletion(true);
		
		
			        
			Job job2 =new Job(conf,"tfidf1Job");
			job2.setJarByClass(org.greenwich.tfidf.tfidfDriver.class);
			
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			
			job2.setMapperClass(tfidfMapper.class);
			job2.setReducerClass(tfidfReducer.class);
	               
	        job2.setMapOutputKeyClass(Text.class);
	        job2.setMapOutputValueClass(Text.class);
	                
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job2, new Path(InvertedIndexOutFolder));
			FileOutputFormat.setOutputPath(job2, new Path(tfidf1OutFolder));
			
			job2.waitForCompletion(true);
			
		
	        
			Job job3 =new Job(conf,"tfidf1Job");
			job3.setJarByClass(org.greenwich.tfidf.tfidfDriver2.class);
			
			job3.setInputFormatClass(KeyValueTextInputFormat.class);
			job3.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			job3.setMapperClass(tfidfMapper2.class);
			job3.setReducerClass(Reducer.class);
	               
	        job3.setMapOutputKeyClass(Text.class);
	        job3.setMapOutputValueClass(Text.class);
	                
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job3, new Path(tfidf2InFolder));
			FileOutputFormat.setOutputPath(job3, new Path(tfidf2OutFolder));
			
			 job3.waitForCompletion(true);
			
			

		   
			Job job4 =new Job(conf,"EuclideanLengthsJob");
			job4.setJarByClass(EuclideanLengthsDriver.class);
			
			job4.setInputFormatClass(SequenceFileInputFormat.class);
			job4.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			job4.setMapperClass(EuclideanLengthsMapper.class);
			job4.setReducerClass(Reducer.class);
	               
	        job4.setMapOutputKeyClass(Text.class);
	        job4.setMapOutputValueClass(Text.class);
	                
			job4.setOutputKeyClass(Text.class);
			job4.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job4, new Path(EuclideanLengthsInFolder));
			FileOutputFormat.setOutputPath(job4, new Path(EuclideanLengthsOutFolder));
			
			return job4.waitForCompletion(true)? 0:1;

		
		
	}
	
	public static void main(String [] args) throws  IOException, InterruptedException , Exception {
		int exitCode = ToolRunner.run(new InvertedIndexDriver(), args);

		MapFileFixer mapFileFixer = new MapFileFixer();
		mapFileFixer.convertSeqToMapFile(new Configuration(), InvertedIndexOutFolder );
		
		mapFileFixer.convertSeqToMapFile(new Configuration(), tfidf2OutFolder );
		
		mapFileFixer.convertSeqToMapFile(new Configuration(), EuclideanLengthsOutFolder );
		System.exit(exitCode);
		
		
	}

}
