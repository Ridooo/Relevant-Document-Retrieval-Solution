package org.greenwich.invertedIndex;

import java.util.ArrayList;
import java.util.List;
import java.io.*;
 


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
public class InvertedIndexUnitTest {

	  MapDriver<Text, Text, Text, TermWritable> mapDriver;
	  ReduceDriver<Text,TermWritable,Text,Text> reduceDriver;
	  MapReduceDriver<Text, Text, Text, TermWritable, Text, Text> mapReduceDriver;
	  
	  @Before
	  public void setUp() {
		  /*
		   * Three key classes in MRUnits are MapDriver for Mapper Testing, ReduceDriver for Reducer Testing and MapReduceDriver for end to end MapReduce Job testing. This is how we will setup the Test Class.
		   */
	    GenerateTermsMapper mapper = new GenerateTermsMapper();
	    GeneratePositingListsReducer reducer = new GeneratePositingListsReducer();
	    mapDriver = MapDriver.newMapDriver(mapper);;
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	  }
	  
	  @Test
	  public void testMap() throws IOException {
	    mapDriver.withInput(new Text("10091"), new Text(
	        "##ENDOFID JO00CAB-147##ENDOFPKEY	Active Directory##ENDOFSUMMARY	2007043010000011##ENDOFDESCRIPTION	We need to test active directory issue for users definition.Assigned to Tawfiq Khalili at 11:00 am. Assigned to Tawfiq Assigned on 30/4/07. This task is currently assigned to Abdul Salam from development team; based on Ahmed Shahrouri notification, his team member is going to test domain users integrity at CAB on thursday 24/05/2007 assigned to Ahmad Shahrori Dear AlMuthanna  Mukahhal,  Sunday Abd Al_Salam will go to the bank to test the problem and fix it    Email: Helpdesk@Progressoft.com Web:http://194.165.134.179/otrs/customer.pl   Abdalsalam is totally dedicated these days to ECC Core development and I have no substitute to him during the coming period. So we need to reschedule his visit to CAB to later appointment that we need to specify later.  Ahmed Farouq Shahrori  Change the Owner and Responsible to Iskandar Odeh. Assigned to Shahrori Dear AlMuthanna  Mukahhal,  We will schedule a visit to our site soon in order to work and solve this problem...    Iskandar Odeh Application TTL ECC Support Team Email: Helpdesk@Progressoft.com Web:http://194.165.134.179/otrs/customer.pl   Abd Salaam is ready to go with you tomorrow morning after 10 amDear Shahrori, why do u keep changing the owner, you are the owner of this problem because its related to you, when its finish i will close it. keep it untill its fixed... no problem my friend. But i assigned the action for you since you need to arrange for the visit to CAB Ahmed, Please keep us posted on the progress of this ticket. Dear Ahmed, can you please provide u with the solution regarding this problem... AbdSalaam has visited the bank and he put the right settings for the active directory depending with the help of Bank support team. However, the active directory returns only 1000 users and that because of a limit in the active directory. So Abd asked them to increase the limit so that active directory return all the users. Then admin of security will be able to view all the bank users. .Dear Iskander,  I think this prblem is the same as in ticket. Please get more information about the problem (logs, screenshots, Steps to reproduce the problem,  ..)from the Bank as we discussed previously and then asign it to me, so we will be able to replicate the case here on our test servers or at leat we can trace the problem. Dear Iskander,  I think this problem is the same as in ticket . Please get more information about the problem (logs, Screenshots, Steps to reproduce the problem,  ..)from the Bank as we discussed previously and then assign it to me, so we will be able to replicate the case here on our test servers or at least we can trace the problem.  Ahmed Farouq Shahrori  We need to test active directory issue for users definition. Assigned to Alex Odeh Al-Muthanna Mukahhal sent an email to the helpdesk indicating that the main problem was resolved but he is facing a new problem related to the same issue again. When he tries to add some users, it gives an error message as the  figure attached. Dear,  I have checked the attached image but still we need to get logs and to get the right sequence to reproduce the problem. We have agreed with support team the first action of investigating a problem must be from support team and then if they did not fix the problem then they give a report about the problem so development team can trace the problem and fix it. Dear Raad,    Regading the Actice directory problem, i need you to investigate more on how this error occured, and try to bring the logs for tomcat and oc4j.  Thanks I called Raad Al-Amad and he told me that he visited the Bank Yesterday with Abdalsalam Al-Smadi; they found an Error in the Code that needs to be modified  Dear AlMuthanna  Mukahhal,  ECC Users using Active Direcory in CAB is Ready now    Please Confirm Resolution.    Raad Al-Amad ECC Support Team Email: Helpdesk@Progressoft.com Web:http://194.165.134.179/otrs/customer.pl   Closed Closed ##ENDOFACTIONBODY"));
	    mapDriver.withOutput(new Text(), new TermWritable());
	    mapDriver.runTest();
	  }
	  /*
	   * above	10098[[<ALPHANUM>,78,510,515$]]
		acl	10105[[<ALPHANUM>,1,0,3$]]
		action	10179[[<ALPHANUM>,28,156,162$]]
		action	10064[[<ALPHANUM>,150,1107,1113$]]
	   */
	  
	  @Test
	  public void testReduce() throws IOException{
	    List<TermWritable> values1 = new ArrayList<TermWritable>();
	    values1.add(new TermWritable("above","<ALPHANUM>",0,3,1,"10105",1));
	    values1.add(new TermWritable("above","<ALPHANUM>",78,510,515,"10098",1));
	    //values.add(new TermWritable("above","<ALPHANUM>",79,517,519,10098));
	    //values.add(new TermWritable("above","<ALPHANUM>",80,317,319,10064));
	    reduceDriver.withInput(new Text("above"), values1);
	    reduceDriver.withOutput(new Text("above"), new Text("[<ALPHANUM>,0,3,1],[<ALPHANUM>,78,510,515],"));
	    
	    reduceDriver.runTest();
	  }
	  
	  public static void main(String[] args)throws IOException {
		  InvertedIndexUnitTest unitTest = new InvertedIndexUnitTest();
		   unitTest.testReduce();
		  //System.out.println(results);
		  
	  }

}
