package edu.uta.cse6331;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Pair implements WritableComparable<Pair> 
{
	  int i;
	  int j;
	  
	public Pair(){}
	  
	public Pair(int iPair,int jPair)
	{
		i = iPair;
		j = jPair;
	}
	@Override
	public void readFields(DataInput in) throws IOException 
	{
		i = in.readInt();
		j = in.readInt();
	}
	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeInt(i);
		out.writeInt(j);
	}
	@Override
	public int compareTo(Pair o)
	{
		int n = this.i - o.i;
		if(n==0) 
			return this.j-o.j;
		else return n;
	}
}

class Elem implements Writable {
	  short tag;  // 0 for M, 1 for N
	  int index;  // one of the indexes (the other is used as a key)
	  double value;
	  
	  public Elem(){}
	  
	  //Constructor
	  public Elem(short t, int i,double v)
	  {
		  tag = t;
		  index = i;
		  value = v;
	  }
	  
	// Deserializes the fields of this object from in
	@Override
	public void readFields(DataInput in) throws IOException 
	{
		tag = in.readShort();
		index = in.readInt();
		value = in.readDouble();
		
	}
	
	//Serializes the fields of this object from out
	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeShort(tag);
		out.writeInt(index);
		out.writeDouble(value);		
	}
	}	

public class Multiply 
{  
	public Multiply(){}
	
	 public static class FirstReducer extends Reducer<IntWritable,Elem,Pair,DoubleWritable>
	 {
		 static Vector<Elem> elemM = new Vector<Elem>();
		 static Vector<Elem> elemN = new Vector<Elem>();
	     
	     public void reduce(IntWritable key,Iterable<Elem> values,Context context) throws IOException,InterruptedException
	     {
	    	 elemM.clear();
	    	 elemN.clear();
	    	 
	    	 for(Elem v:values)
	    	 {   	
	    		 Elem eOne = new Elem(v.tag,v.index,v.value);
	    		if(v.tag==0)
	    		{		
	    			
	    			System.out.println("E1 inde:"+eOne.index+","+eOne.value);
	    			elemM.add(eOne);
	    		}
	    		else 
	    		{
	    			elemN.add(eOne);

	    		}
	    	 }
	    	 
	    	 for(Elem e1:elemM )
	    	 {   
	    		 Elem elemTmpOne = new Elem(e1.tag,e1.index,e1.value);
	    		 for(Elem e2:elemN)
	    		 {   
	    			 Elem elemTmpTwo = new Elem(e2.tag,e2.index,e2.value);
	    			 System.out.println(elemTmpOne.index+","+elemTmpTwo.index+","+elemTmpOne.value+","+elemTmpTwo.value+","+elemTmpOne.value*elemTmpTwo.value);
	    		
	    			 context.write(new Pair(elemTmpOne.index,elemTmpTwo.index), new DoubleWritable(elemTmpOne.value*elemTmpTwo.value));
	    		 }
	    	 }
	     }	 
	 }
	 
	
		
	 public static class MMapperClass extends Mapper<Object,Text,IntWritable,Elem>
	 {
		 public void map(Object key,Text value,Context context) throws IOException,InterruptedException
		 {
			 Scanner s = new Scanner(value.toString()).useDelimiter(",");
			 short tag = 0;
			 int i = s.nextInt();
			 int j = s.nextInt();
			 double v = s.nextDouble();		
			 System.out.println(i+","+j);
			 context.write(new IntWritable(j), new Elem(tag,i,v));
			 s.close();
		 }
	 }
	 public static class NMapperClass extends Mapper<Object,Text,IntWritable,Elem> 
	 {
		 public void map(Object key,Text value,Context context) throws IOException,InterruptedException
		 {
			 Scanner s = new Scanner(value.toString()).useDelimiter(",");
			 short tag = 1;
			 int i = s.nextInt();
			 int j = s.nextInt();
			 double v = s.nextDouble();	
			 System.out.println(i+","+j);
			 context.write(new IntWritable(i),new Elem(tag,j,v));
			 s.close();
		 }
	 }
	 

	 public static class SecondMapperClass extends Mapper<Pair,DoubleWritable,Pair,DoubleWritable>
	 {
		 public void map(Pair pair,DoubleWritable value,Context context) throws IOException,InterruptedException
		 {

			 context.write(pair,value);			
		 }
	 }
	 
	 public static class SecondReducer extends Reducer<Pair,DoubleWritable,Text,String>
	 {
		 public void reduce(Pair key,Iterable<DoubleWritable> values,Context context) throws IOException,InterruptedException
	     {
			double m = 0;
			
			for(DoubleWritable v:values)
			{
				m = m + Double.parseDouble(v.toString());
			}
			Pair p = new Pair(key.i,key.j);
			context.write(new Text(p.i+","+p.j+","+m),"");
			
	     }
	 }
    public static void main ( String[] args ) throws Exception
    {
        Job job1 = Job.getInstance();
        job1.setJobName("MultiplyJobOne");
        
        job1.setJarByClass(Multiply.class);
        //For Reducer
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(DoubleWritable.class);
        
        //ForMapper
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Elem.class);
        
        
        job1.setReducerClass(FirstReducer.class);
       
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,MMapperClass.class);
        MultipleInputs.addInputPath(job1,new Path(args[1]),TextInputFormat.class,NMapperClass.class);
       
        
        //FileOutputFormat.setOutputPath(job1,new Path("src/output.txt"));
        FileOutputFormat.setOutputPath(job1,new Path(args[2]));
        boolean success = job1.waitForCompletion(true);
        
        if(success)
        {
        	 Job job2 = Job.getInstance();
        	 job2.setJobName("MultiplyJobOne");
             
        	 job2.setJarByClass(Multiply.class);
             //For Reducer
        	 job2.setOutputKeyClass(Text.class);
        	 job2.setOutputValueClass(String.class);
             
             //ForMapper
        	 job2.setMapOutputKeyClass(Pair.class);
        	 job2.setMapOutputValueClass(DoubleWritable.class);
             
        	 job2.setMapperClass(SecondMapperClass.class);
        	 job2.setReducerClass(SecondReducer.class);
             
        	 job2.setInputFormatClass(SequenceFileInputFormat.class);
        	 job2.setOutputFormatClass(TextOutputFormat.class);
                     	 
        	 FileInputFormat.setInputPaths(job2,new Path(args[2]));
             FileOutputFormat.setOutputPath(job2,new Path(args[3]));
        	 
             job2.waitForCompletion(true);
        }
        
    
    }
    	
}
