# Information retrieval system

Information retrieval system to find top related historical helpdesk tickets for a requested subject. The final implementation of this project is a simplified form of a search engine. 
The system has two core components:
- A distributed inverted index.
- Java API for query processing and documents searching using vector space model.

The project gave me the opportunity to learn and develop knowledge in the following areas:

**_Theoretical:_**
1. An in-depth theoretical understanding of designing and implementing systems for gathering, indexing and searching documents at large scale; methods for evaluating the system and how to use machine learning for text clustering and classification.

**_Technical:_**
* Java programming.
* MapReduce programming paradigm; building MapReduce jobs for analyzing the ingested text, indexing and weighting document terms based on a customized version of TF-IDF model.
* Extensive knowledge in using regular expressions.
* [Apache Lucene](https://lucene.apache.org/); built a customized implementation of a text analyzer.
* Hadoop Operations; setting up, maintaining and monitoring a Hadoop Cluster.
* Using Apache Sqoop to ingest data from RDBMS to HDFS.