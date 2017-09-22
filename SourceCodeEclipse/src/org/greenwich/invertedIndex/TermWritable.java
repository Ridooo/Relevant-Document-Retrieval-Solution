package org.greenwich.invertedIndex;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class TermWritable implements WritableComparable<TermWritable> {
    
    Text        term;
    Text        type;
    IntWritable startOffset;
    IntWritable endOffset;
    IntWritable position;
    Text docID;
    IntWritable location;
    
    //All Writable implementations must have a default constructor so that the MapReduce framework can instantiate them
    public TermWritable() {
        set(new Text(), new Text(), new IntWritable(), new IntWritable(), new IntWritable(), new Text(), new IntWritable());
    }
     public TermWritable(String term, String type, int startOffset, int endOffset, int position, String docID, int location ) {
        set(new Text(term), new Text(type), new IntWritable(startOffset), new IntWritable(endOffset), new IntWritable(position), new Text(docID),  new IntWritable(location));
    }
    public TermWritable(Text term, Text type, IntWritable startOffset, IntWritable endOffset, IntWritable position, Text docID, IntWritable location) {
        set(term, type, startOffset, endOffset, position, docID, location);
    }
    
    public void set(Text term, Text type, IntWritable startOffset, IntWritable endOffset, IntWritable position, Text docID, IntWritable location){
        this.term = term;
        this.type = type;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.position = position;
        this.docID = docID;
        this.location =location;
    }
    
    public void setTerm(String term){
        this.term = new Text(term);
    }
    public void setTerm(Text term){
        this.term = term;
    }
    public void setType(String type){
        this.type= new Text(type);
    }
    public void setType(Text type){
        this.type= type;
    }
     public void setStartOffset(int startOffset){
        this.startOffset = new IntWritable(startOffset);
    }
    public void setStartOffset(IntWritable startOffset){
        this.startOffset= startOffset;
    }
    public void setEndOffset(int endOffset){
        this.endOffset= new IntWritable(endOffset);
    }
    public void setEndOffset(IntWritable endOffset){
        this.endOffset = endOffset;
    }
     public void setPosition(int position){
        this.position= new IntWritable(position);
    }
    public void setPosition(IntWritable position){
        this.position= position;
    }
    public void setDocID(String docID){
        this.docID = new Text(docID);
    }
    public void setDocID(Text docID){
        this.docID= docID;
    }
    public void setLocation(int location){
        this.location = new IntWritable(location);
    }
    public void setLocation(IntWritable location){
        this.location= location;
    }
    public Text getTerm() {
    return term;
    }
    public Text getType() {
    return type;
    }
    public IntWritable getStartOffset() {
    return startOffset;
    }
    public IntWritable getEndtOffset() {
    return endOffset;
    }
    public IntWritable getPosition() {
    return position;
    }
    public Text getDocID() {
    return docID;
    }
    public IntWritable getLocation() {
        return location;
        }
    
    
  //  TermWritable’s write() method serializes each Text and IntWritable objects in turn to the output stream, by delegating to the Text and IntWritable objects themselves
    @Override
    public void write(DataOutput out) throws IOException {
    term.write(out);
    docID.write(out); 
    position.write(out);
    startOffset.write(out);
    endOffset.write(out);
    type.write(out);
    location.write(out);
    
    }
    //Similarly, readFields() deserializes the bytes from the input stream by delegating to each Text and IntWritable objects.
    @Override
    public void readFields(DataInput in) throws IOException {
    term.readFields(in);
    docID.readFields(in);
    position.readFields(in);
    startOffset.readFields(in);
    endOffset.readFields(in);
    type.readFields(in);
    location.readFields(in);
 
    }
    
    /* The hash Code() method is used by the HashPartitioner (the default partitioner in MapReduce) to choose a reduce partition, so you should make sure that you write a good hash
function that mixes well to ensure reduce partitions are of a similar size.*/
    @Override
    public int hashCode() {
    //return term.hashCode() * 163 + type.hashCode();
          return term.hashCode() ;
    }
    @Override
    public boolean equals(Object o) {
    if (o instanceof TermWritable) {
    TermWritable tw = (TermWritable) o;
    return term.equals(tw.term) && docID.equals(tw.docID) && position.equals(tw.position);
     }
    return false;
    }
    //TextOutputFormat calls  toString() on keys and values for their output representation. For Text Pair, we write the underlying Text objects as strings separated by a tab character.
    @Override
    public String toString() {
    return docID + "[[" + type + "," + position + "," + startOffset + "," + endOffset + "," + location + "$]]";
     //return docID + "\t";
    }
    
    //TextPair is an implementation of WritableComparable, so it provides an implementation of the compareTo() method that imposes the ordering you would expect:
    @Override
    public int compareTo(TermWritable tp) {
    int cmp = term.compareTo(tp.term);
   if (cmp != 0) {
    return cmp;
    }
    int cmp1 = docID.compareTo(tp.docID);
    if (cmp1 != 0) {
    return cmp1;
    }
    return position.compareTo(tp.position);
   // return cmp;
    }
    
    
    
    /*
    compare two TermWritable objects just by looking at their serialized representations.
    It turns out that we can do this, since TermWritable is the concatenation of three Text objects and three IntWritable objects,
    and the binary representation of a Text object is a variable-length integer containing
    the number of bytes in the UTF-8 representation of the string, followed by the UTF-8
    bytes themselves. The trick is to read the initial length, so we know how long the "term"  Text object’s byte representation is;
    */
    
    public static class Comparator extends WritableComparator {
        
        private static final Text.Comparator Text_Comparator = new Text.Comparator();
        private static final IntWritable.Comparator IntWritable_Comparator = new IntWritable.Comparator();
        public Comparator() {
            super(TermWritable.class);
        }
        
        public int compare(byte[] b1, int s1,int l1,
                           byte[] b2, int s2, int l2) {
            try {
                
                /* The subtle part of this code is calculating firstL1 and firstL2, the lengths of the term 
                Text field in each byte stream. Each is made up of the length of the variable-length integer 
                (returned by decodeVIntSize() on WritableUtils) and the value it is encoding (returned by readVInt()). */
                
                int termL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1,s1) ;
                int termL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2,s2) ;
                int cmp = Text_Comparator.compare(b1, s1, termL1, b2, s2, termL2);
                if ( cmp != 0){
                    return cmp;
                }
                // Since the representation binary on IntWritable is 4 bytes then the length of the second field docID is  s1+firstL1,firstL1+4.
                // firstL1 is the length of the binary representation of the first field term.
                

                int docIDL1 = WritableUtils.decodeVIntSize(b1[s1+termL1]) + readVInt(b1,s1+termL1) ;
                int docIDL2 = WritableUtils.decodeVIntSize(b2[s2+termL2]) + readVInt(b2,s2+termL2) ;
                int cmp1 = Text_Comparator.compare(b1, s1+termL1, docIDL1, b2, s2+termL2, docIDL2);
                 if ( cmp1 != 0){
                   return cmp1;
                }
                return IntWritable_Comparator.compare(b1, docIDL1, docIDL1+4, b2,  docIDL2, docIDL2+4);
            }catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
            
        }
         @Override
        public int compare(WritableComparable a, WritableComparable b) {
        if (a instanceof TermWritable && b instanceof TermWritable) {
            int cmp = ((TermWritable) a).term.compareTo(((TermWritable)b).term);
            if (cmp != 0) {
            return cmp;
            }
            int cmp1 = ((TermWritable) a).docID.compareTo(((TermWritable)b).docID);
            if (cmp1 != 0) {
            return cmp1;
            }
            return ((TermWritable) a).position.compareTo(((TermWritable)b).position);
            }
        return super.compare(a, b);
        }
    }
    //MapReduce sees the TermAttribute class, it knows to use the raw comparator as its default comparator.
    
    static{
        WritableComparator.define(TermWritable.class, new Comparator());
    }
}
