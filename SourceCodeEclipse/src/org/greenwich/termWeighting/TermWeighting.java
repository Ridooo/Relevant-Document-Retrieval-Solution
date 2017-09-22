package org.greenwich.termWeighting;



public class TermWeighting {
	
	
	public TermWeighting(){
		
	}

	public Float calTermDocWeight(long N, int termFreq, long termDocFreq, int termLocationCount){
		
		Float weightTermFreq =(float) Math.log(termFreq+termLocationCount);
		Float weightInverseDocFreq = (float) Math.log(N+1/termDocFreq);
		
		return weightTermFreq*weightInverseDocFreq;
	}
	
	public Float calTermQueryWeight(long N, int termFreq, long termDocFreq){
		
		Float weightTermFreq = (float) Math.log(termFreq+3);
		Float weightInverseDocFreq = (float) Math.log(N+1/termDocFreq);
		
		return weightTermFreq*weightInverseDocFreq;
	}
}
