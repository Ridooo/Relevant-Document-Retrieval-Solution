package org.greenwich.tfidf;


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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.greenwich.mapFile.MapFileFixer;


public class EuclideanLengthsDriver extends Configured implements Tool{
	
	
	public int run(String [] args) throws IOException, InterruptedException , Exception{
		
		/*if(args.length != 2) {
			System.err.printf("Usage: %s [generic-options] <INPUT> <OUTPUT>", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}*/
		
        Configuration conf =getConf();
        conf.set("fs.default.name", "hdfs://localhost:8020"); 
		conf.set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
		conf.set("mapreduce.map.log.level", "ALL");
        

		Path  path = new Path(args[1]);
		FileSystem fs = path.getFileSystem(conf);
		if (fs.exists(path)){
			fs.delete(path,true);
	        	
		}
		
        
		Job job =new Job(conf,"EuclideanLengthsJob");
		job.setJarByClass(getClass());
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(EuclideanLengthsMapper.class);
		job.setReducerClass(Reducer.class);
               
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
                
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost/user/admin/tfidf2OutFolder"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost/user/admin/EuclideanLengthsOutFolder"));
		
		return job.waitForCompletion(true)? 0:1;
	}
	
	public static void main(String [] args) throws  IOException, InterruptedException , Exception {
		int exitCode = ToolRunner.run(new EuclideanLengthsDriver(), args);
		MapFileFixer mapFileFixer = new MapFileFixer();
		
		
		mapFileFixer.convertSeqToMapFile(new Configuration(), "hdfs://localhost/user/admin/EuclideanLengthsOutFolder" );
		System.exit(exitCode);
		
		
	}

}
