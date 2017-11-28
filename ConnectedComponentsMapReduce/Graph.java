package edu.uta.cse6331;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Scanner;
import java.util.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

class Vertex implements Writable
{

	short tag;
	long group;
	long VID;
	Vector<Long> adjacent;

	
	public Vertex(){}
	
	public Vertex(short tagV,long groupV,long VIDV,Vector<Long> adjacentV)
	{
		tag = tagV;
		group = groupV;
		VID = VIDV;
		adjacent = adjacentV;
	}
	
	public Vertex(short tagV,long groupV)
	{
		tag = tagV;
		group = groupV;
	}

	@Override
	public void readFields(DataInput in) throws IOException {		
		
		 tag = in.readShort();
		 
		 if(tag == 0){
		 group = in.readLong();
		 VID = in.readLong();
		 adjacent = readVector(in);}
		 else
		 {
			 group = in.readLong();
		 }
		 
	}
	
	public static Vector<Long> readVector(DataInput in) throws IOException 
	{
        int length = in.readInt();
        Vector<Long> vector = new Vector<Long>();
            
            for (int i = 0; i < length; i++) 
            {
                vector.add(in.readLong());
            }
        return vector;
    }
    
	@Override
	public void write(DataOutput out) throws IOException
	{
		if(tag == 0){
		out.writeShort(tag);
		out.writeLong(group);
		out.writeLong(VID);
		writeVector(this.adjacent,out);}
		else
		{
			out.writeShort(tag);
			out.writeLong(group);
		}
		
	}
	
	public static void writeVector(Vector<Long> vector,DataOutput out) throws IOException
	{
		 out.writeInt(vector.size());
		
		 for (int i = 0; i < vector.size(); i++)
		 {
             out.writeLong(vector.get(i));
         }
		
	}
	
	public String toString() {
		return tag+","+group+","+VID+","+adjacent;
	}
	
}
public class Graph extends Configured implements Tool
{
	public Graph(){}
	
	public static class FirstMapper extends Mapper<Object,Text,LongWritable,Vertex>
	{
		  public void map ( Object key, Text value, Context context )
                  throws IOException, InterruptedException
		 {
			  Scanner s = new Scanner(value.toString()).useDelimiter(",");
			  long VID = s.nextLong();
			  Vector<Long> adjacent = new Vector<Long>();
			  while (s.hasNext()) 
			  {
				    Long nextVid = s.nextLong();
				    adjacent.add(nextVid);
			  }
			  short tag = 0;
			  Vertex vertex = new Vertex(tag,VID,VID,adjacent);
			  //System.out.println(vertex.tag+","+vertex.group+","+vertex.VID+","+vertex.adjacent);
			  context.write(new LongWritable(VID), vertex);
			  
			  s.close();
		 }
	}
	
	public static class FirstReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex>
	  {
	        @Override

	        public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
	                          throws IOException, InterruptedException  
	        {
	        
	        for (Vertex val: values)
	        {
	        	Vertex v = new Vertex(val.tag,val.group,val.VID,val.adjacent);
	        	//System.out.println(v.tag+","+v.group+","+v.VID+","+v.adjacent);
	        	for(Long k: v.adjacent)
	        	{
	        		System.out.println(k);
	        	}
	        	context.write(key, v);
	        }
	        
	        }
	  }
	
	
	public static class SecondMapper extends Mapper<LongWritable,Vertex,LongWritable,Vertex>
	{
		  public void map ( LongWritable key, Vertex value, Context context )
                  throws IOException, InterruptedException
		 {
			  Vertex vertex = new Vertex(value.tag,value.group,value.VID,value.adjacent);
			  context.write(new LongWritable(vertex.VID), vertex);
			  
			  for(Long n: vertex.adjacent)
			  {
				  short tag = 1;
				  context.write(new LongWritable(n), new Vertex(tag,vertex.group));
			  }
			
		 }
	}
	
	
	public static class SecondReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex>
	  {
        @Override
        public void reduce(LongWritable key, Iterable<Vertex> values, Context context )
                          throws IOException, InterruptedException  
        {
        	long m = Long.MAX_VALUE;
        	Vector<Long> adj = new Vector<Long>();
        	
        	for(Vertex v: values)
        	{
        		Vertex vertex;
        		
       		
        		if(v.tag==0)
        		{
        			vertex = new Vertex(v.tag,v.group,v.VID,v.adjacent);
        			adj = (Vector<Long>) vertex.adjacent.clone();
        		}
        		else
        		{
        			vertex = new Vertex(v.tag,v.group);
        		}
        		
        		m = Math.min(m, vertex.group);
        	}
        	
        	short tag = 0;
        	context.write(new LongWritable(m), new Vertex(tag,m,key.get(),adj));
        }
        
      }
	
	public static class ThirdMapper extends Mapper<LongWritable,Vertex,LongWritable,IntWritable>
	{
		  public void map ( LongWritable key, Vertex value, Context context )
                  throws IOException, InterruptedException
		 {
			context.write(key, new IntWritable(1));  
		 }
	}
	public static class ThirdReducer extends Reducer<LongWritable,IntWritable,LongWritable,IntWritable>
	  {
      @Override
      public void reduce(LongWritable key, Iterable<IntWritable> values, Context context )
                        throws IOException, InterruptedException  
      {
    	  int m = 0;
    	  
    	  for(IntWritable v :values)
    	  {
    		  m = m+v.get();
    	  }
    	  
    	  context.write(key, new IntWritable(m));
      }
      
      }
	
	@Override
	public int run(String[] args) throws Exception 
	{
		 Configuration conf = getConf();
    	 Job job1 = Job.getInstance(conf);
    	 
    	 job1.setJobName("GraphJob");
    	 job1.setJarByClass(Graph.class);
         
    	 job1.setMapOutputKeyClass(LongWritable.class);
    	 job1.setMapOutputValueClass(Vertex.class);
    	 
    	 job1.setOutputKeyClass(LongWritable.class);
    	 job1.setOutputValueClass(Vertex.class);
    	 
    	 job1.setMapperClass(FirstMapper.class);
    	 job1.setReducerClass(FirstReducer.class);

    	 job1.setInputFormatClass(TextInputFormat.class);
    	 job1.setOutputFormatClass(SequenceFileOutputFormat.class);
    	 
         //FileInputFormat.setInputPaths(job1,new Path("/Users/archana/Desktop/CloudComp/project2/small-graph.txt"));
    	 FileInputFormat.setInputPaths(job1,new Path(args[0]));
    	 FileOutputFormat.setOutputPath(job1,new Path(args[1]+"/f0"));
         
         job1.waitForCompletion(true);
         
         for(int i=0;i<5;i++)
         {
         Job job2 = Job.getInstance(conf);
    	 
         job2.setJobName("GraphJob");
         job2.setJarByClass(Graph.class);
         
         job2.setMapOutputKeyClass(LongWritable.class);
         job2.setMapOutputValueClass(Vertex.class);
    	 
         job2.setOutputKeyClass(LongWritable.class);
         job2.setOutputValueClass(Vertex.class);
    	 
         job2.setMapperClass(SecondMapper.class);
         job2.setReducerClass(SecondReducer.class);

         job2.setInputFormatClass(SequenceFileInputFormat.class);
        
         job2.setOutputFormatClass(SequenceFileOutputFormat.class);
     
    	 
         FileInputFormat.setInputPaths(job2,new Path(args[1]+"/f"+i));
         FileOutputFormat.setOutputPath(job2,new Path(args[1]+"/f"+(i+1)));
         
         job2.waitForCompletion(true);        	 
         }
         
    	 Job job3 = Job.getInstance(conf);
    	 
    	 job3.setJobName("GraphJob");
    	 job3.setJarByClass(Graph.class);
         
    	 job3.setMapOutputKeyClass(LongWritable.class);
    	 job3.setMapOutputValueClass(IntWritable.class);
    	 
    	 job3.setOutputKeyClass(LongWritable.class);
    	 job3.setOutputValueClass(IntWritable.class);
    	 
    	 job3.setMapperClass(ThirdMapper.class);
    	 job3.setReducerClass(ThirdReducer.class);

    	 job3.setInputFormatClass(SequenceFileInputFormat.class);
    	 job3.setOutputFormatClass(TextOutputFormat.class);
    	 
         FileInputFormat.setInputPaths(job3,new Path(args[1]+"/f5"));
         FileOutputFormat.setOutputPath(job3,new Path(args[2]));
         
         job3.waitForCompletion(true);
         
         return 0;
		
	}
	
    public static void main ( String[] args ) throws Exception 
    {
    	ToolRunner.run(new Configuration(), new Graph(), args);
    }

	
}
