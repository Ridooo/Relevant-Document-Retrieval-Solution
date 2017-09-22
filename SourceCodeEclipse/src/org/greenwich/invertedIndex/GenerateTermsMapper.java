package org.greenwich.invertedIndex;


import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Version;
import org.greenwich.analyzers.TicketStandardAnalyzer;

@SuppressWarnings("deprecation")
public class GenerateTermsMapper extends Mapper<Text, Text, Text, TermWritable>{
	
	 private TicketStandardAnalyzer ticketstandardanalyzer =new TicketStandardAnalyzer(Version.LUCENE_30); ;
	
	public void map(Text key, Text value, Context context)  throws IOException,  InterruptedException {
		
 
		String[] value0 = value.toString().split("#ETSM#");
		String ticketSummary = value0[0];
		String[] value1 =  value0[1].split("#ETDS#");
		
		String ticketDescription =value1[0];
		String ticketComment = value1[1];
		
		String[] Values = {ticketSummary,ticketDescription,ticketComment};
		
		
		for(int i=0 ; i< Values.length; i++) {
			
			TokenStream standardtokenstream = ticketstandardanalyzer.tokenStream("contents", new StringReader(Values[i].toString()));
			TermAttribute term = standardtokenstream.addAttribute(TermAttribute.class);
			PositionIncrementAttribute posIncr = standardtokenstream.addAttribute(PositionIncrementAttribute.class);
			OffsetAttribute offset = standardtokenstream.addAttribute(OffsetAttribute.class);
			TypeAttribute type = standardtokenstream.addAttribute(TypeAttribute.class);
			
                                                          
			int position =0;
                         
			while (standardtokenstream.incrementToken()) {
				int increment = posIncr.getPositionIncrement();
				if ( increment > 0) {
					position = position + increment;
				}
		
                                    posIncr.setPositionIncrement(position);
			        
                                   
                                   TermWritable tw = new TermWritable();
                                            tw.setTerm(term.term());
                                            tw.setType(type.type());
                                            tw.setStartOffset(offset.startOffset());
                                            tw.setEndOffset(offset.endOffset());
                                            tw.setPosition(posIncr.getPositionIncrement());
                                            tw.setDocID(key.toString());
                                            tw.setLocation(Values.length - i);
                                            context.write(tw.getTerm(),tw);
                                            
                             } 
        
			
		}

				                   
		
		
		
		 
					

				
		
		
	}

}
