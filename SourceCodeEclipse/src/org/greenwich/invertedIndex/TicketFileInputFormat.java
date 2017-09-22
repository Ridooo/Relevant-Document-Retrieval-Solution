package org.greenwich.invertedIndex;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class TicketFileInputFormat extends FileInputFormat<Text,Text>{
	private TicketRecordReader ticketRecordReader = null;
	
	public RecordReader<Text,Text> createRecordReader(
			InputSplit inputsplit, TaskAttemptContext context)
					throws IOException,InterruptedException {
		ticketRecordReader = new TicketRecordReader();
		ticketRecordReader.initialize(inputsplit, context);
		return ticketRecordReader;
	}

}
