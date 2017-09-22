package org.greenwich.invertedIndex;
//import java.io.*;
import java.lang.StringBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class TicketRecordReader extends RecordReader<Text,Text>{
	private FileSplit filesplit;
	private Configuration conf;
	private Text value ;
        
	private static final Pattern regexPattern1 = Pattern.compile("([\\d]+)\\s+##ENDOFID\\s+([a-zA-Z0-9]+-[0-9]+)##ENDOFPKEY\\s+(.*)##ENDOFSUMMARY\\s+(.*)##ENDOFDESCRIPTION\\s+(.*)##ENDOFACTIONBODY$");
	private Text currentKey;
	
	private BufferedReader bufferedreader;
	private	int inputCountSofar=0;
	private StringBuilder issueID ;
	private StringBuilder ticketID ;
	private StringBuilder ticketSummary;
	private StringBuilder ticketDescription;
	private StringBuilder ticketComment;
   	private String line = null;
       
	@Override
	public void initialize(InputSplit inputsplit,TaskAttemptContext context) throws IOException, InterruptedException{
		this.filesplit = (FileSplit) inputsplit;
		this.conf = context.getConfiguration();
                Path file = this.filesplit.getPath();
		FileSystem filesystem = FileSystem.get(conf);
		FSDataInputStream inputstream =filesystem.open(file);
		bufferedreader = new BufferedReader ( new InputStreamReader(inputstream),1024*100);
		while ((line = bufferedreader.readLine())!= null) {
			Matcher matcher = regexPattern1.matcher(line);
			if(matcher.matches()){
				break;
			}
		}
	}
	@Override
	public boolean nextKeyValue()  throws IOException, InterruptedException {
        inputCountSofar++;
		issueID = new StringBuilder();
		ticketID = new StringBuilder();
        ticketSummary = new StringBuilder();
		ticketDescription =new StringBuilder();
		ticketComment = new StringBuilder();
		
            while ( (line = bufferedreader.readLine() ) != null) {
			Matcher matcher1 = regexPattern1.matcher(line);
			if (matcher1.matches()) {
				issueID = issueID.append(matcher1.group(1));
				 ticketID =  ticketID.append(matcher1.group(2));
				 ticketSummary = ticketSummary.append(matcher1.group(3));
				 ticketDescription = ticketDescription.append(matcher1.group(4));
				 ticketComment = ticketComment.append(matcher1.group(5));
				 currentKey = new Text(ticketID.toString());
                                 // the reason behind having the summary, descrption and comment values segregated, it is the anticipation of applying different text anaylizing during indexing.
                                 value= new Text(ticketSummary.toString() + " #ETSM# " + ticketDescription.toString()+ " #ETDS# " + ticketComment.toString());
                                // value= new Text(ticketSummary.toString() + " #ETSM# " + ticketDescription.toString());
                        }else {
                        	 //System.out.println(line);
                        	 continue;
                         }
                                
                                
                                return true;
                       } 
                        System.out.println("Line processing ends halfway through");
                	return false;
			}
	
		
	
	@Override
	public Text getCurrentKey() {
		return currentKey;
	}
	
	@Override
	public Text getCurrentValue() {
		return value;
	}
	
	@Override
	public float getProgress() {
		      return inputCountSofar;
	}
	
	@Override
	public void close()  throws IOException{
            bufferedreader.close();
		
	}
	
	

}
