
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PageRank extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
    }
    //This is to create consistent file names at each iteration
    private static NumberFormat format4iterFiles = new DecimalFormat("00");   
	private static int N;
    
	@Override
    public int run(String[] args) throws Exception {
		
       boolean Job1Done = parseXml(args[0],args[1]+"tmp/Job1",
        							args[1]+"results/rawdata");  //Read in the input from the given hierarchy
        //Job2
        boolean Job2Done = getadjacencyGraph(args[1]+"results/rawdata",args[1]+"tmp/Job2",
        									args[1]+"results/PageRank.outlink.out");
        
        //Job3:
        N = calTotalPages(args[1]+"results/PageRank.outlink.out",args[1]+"tmp/Job3",
						 args[1]+"results/PageRank.n.out");
       
        
        //PreProcessing for Job4
		boolean done = preprocessAdjGraphForJob4(args[1]+"results/PageRank.outlink.out",
		//boolean done = preprocessAdjGraphForJob4(args[1]+"data/dummyAdjGraph",
												 args[1]+"tmp/preprocessJob4",
												 args[1]+"tmp/Job4/iter00/prepiter00");
		if(!done)  return -1;
		
		String lastResultPath = null;
		for (int runs = 0; runs < 8; runs++) {
			String inPath = args[1]+"tmp/Job4/iter" + format4iterFiles.format(runs);
			lastResultPath = args[1]+"tmp/Job4/iter" + format4iterFiles.format(runs + 1);

			done = calPageRank(inPath, lastResultPath);  
			
			if (!done) return -1;
			if (runs==0 || runs==7){
				//Run Job 5 to get the results of iter1 and iter8 in order
				done = orderRank(lastResultPath,args[1]+"tmp/Job5",args[1]+"results/PageRank.iter"+ (runs+1) +".out");
				}		
		} 
		return 0;
    }

	
	private boolean preprocessAdjGraphForJob4 (String inputPath, String outputPath, String CopyPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("NumberOfNodes", Float.toString(N)); //To pass the number of nodes
		conf.set("mapreduce.output.textoutputformat.separator", "@"); //To place @ as separator
		Job prepJob4 = Job.getInstance(conf, "PreProcess for Job4");
		prepJob4.setJarByClass(PageRank.class);

		prepJob4.setOutputKeyClass(Text.class);
		prepJob4.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(prepJob4, new Path(inputPath));
		FileOutputFormat.setOutputPath(prepJob4, new Path(outputPath));

		prepJob4.setMapperClass(mapperPreprocessJob4.class);
		//prepJob4.setNumReduceTasks(3); 
		prepJob4.setReducerClass(reducerPreprocessJob4.class);
		boolean success = prepJob4.waitForCompletion(true);
		
		Path srcPath = new Path(outputPath);
		Path dstPath = new Path(CopyPath);
		FileSystem fs_src = srcPath.getFileSystem(conf);
		FileSystem fs_dst = dstPath.getFileSystem(conf);
		FileUtil.copyMerge(fs_src, srcPath, fs_dst, dstPath, true, conf, null);

		return success;
	}
	
    public boolean parseXml(String inputPath, String outputPath, String CopyPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
        Job Job1 = Job.getInstance(conf, "Job1 Remove Red Links and Duplicates");
        Job1.setJarByClass(PageRank.class);
        Job1.setMapperClass(mapperJob1.class);
        Job1.setReducerClass(reducerJob1.class);
        FileInputFormat.addInputPath(Job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(Job1, new Path(outputPath));
        Job1.setInputFormatClass(XmlInputFormat.class);
        Job1.setOutputFormatClass(TextOutputFormat.class);
        Job1.setMapOutputKeyClass(Text.class);
        Job1.setOutputKeyClass(Text.class);
        Job1.setOutputValueClass(Text.class);
        Job1.waitForCompletion(true);
		boolean success = Job1.waitForCompletion(true);
		
		Path srcPath = new Path(outputPath);
		Path dstPath = new Path(CopyPath);
		FileSystem fs_src = srcPath.getFileSystem(conf);
		FileSystem fs_dst = dstPath.getFileSystem(conf);
		FileUtil.copyMerge(fs_src, srcPath, fs_dst, dstPath, true, conf, null);

		return success;
    }
    
    public boolean getadjacencyGraph(String inputPath, String outputPath, String CopyPath) throws IOException, ClassNotFoundException, InterruptedException {
    	Configuration conf = new Configuration();
		Job Job2 = Job.getInstance(conf, "Job2 Gen Adj Graph");
		Job2.setJarByClass(PageRank.class);
		Job2.setMapperClass(mapperJob2.class);
		Job2.setReducerClass(reducerJob2.class);
		Job2.setOutputKeyClass(Text.class);
		Job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(Job2, new Path(inputPath));
		FileOutputFormat.setOutputPath(Job2, new Path(outputPath));
		boolean success = Job2.waitForCompletion(true);
		
		Path srcPath = new Path(outputPath);
		Path dstPath = new Path(CopyPath);
		FileSystem fs_src = srcPath.getFileSystem(conf);
		FileSystem fs_dst = dstPath.getFileSystem(conf);
		FileUtil.copyMerge(fs_src, srcPath, fs_dst, dstPath, true, conf, null);
		
		return success;

    }

    
    public int calTotalPages(String inputPath, String outputPath, String CopyPath) throws IOException, ClassNotFoundException, InterruptedException {
    	Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", "="); //To write N=20
		Job Job3 = Job.getInstance(conf, "Job3 Compute Number of Nodes");
		Job3.setJarByClass(PageRank.class);
		Job3.setMapperClass(mapperJob3.class);
		Job3.setNumReduceTasks(1);   				//To enforce a single reducer for returning N
		Job3.setReducerClass(reducerJob3.class);
		Job3.setOutputKeyClass(Text.class);
		Job3.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(Job3, new Path(inputPath));
		FileOutputFormat.setOutputPath(Job3, new Path(outputPath));
		Job3.waitForCompletion(true);

		Path srcPath = new Path(outputPath);
		Path dstPath = new Path(CopyPath);
		FileSystem fs_src = srcPath.getFileSystem(conf);
		FileSystem fs_dst = dstPath.getFileSystem(conf);
		FileUtil.copyMerge(fs_src, srcPath, fs_dst, dstPath, true, conf, null);
		
		long sumLong = Job3.getCounters().findCounter("NumPages","NumPages").getValue();
	    return ((int)sumLong);
		
    }
	private boolean calPageRank(String inputPath, String outputPath) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("NumberOfNodes", Float.toString(N)); //To pass the number of nodes
		conf.set("mapreduce.output.textoutputformat.separator", "@"); //To write N=20
		Job Job4 = Job.getInstance(conf, "Job4");
		Job4.setJarByClass(PageRank.class);

		Job4.setOutputKeyClass(Text.class);
		Job4.setOutputValueClass(Text.class);

		  			
		
		FileInputFormat.setInputPaths(Job4, new Path(inputPath));
		FileOutputFormat.setOutputPath(Job4, new Path(outputPath));

		Job4.setMapperClass(mapperJob4.class);
		//Job4.setNumReduceTasks(3); 
		Job4.setReducerClass(reducerJob4.class);

		return Job4.waitForCompletion(true);
		
	}
	private boolean orderRank(String inputPath, String outputPath, String CopyPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("NumberOfNodes", Float.toString(N)); //To pass the number of nodes
		
		Job Job5 = Job.getInstance(conf, "Job5");
		Job5.setJarByClass(PageRank.class);
		
		Job5.setSortComparatorClass(DescendingComp.class);

		Job5.setMapOutputKeyClass(FloatWritable.class); //sets output key class of Mapper
		Job5.setMapOutputValueClass(Text.class); //
		Job5.setOutputKeyClass(Text.class);  //sets output key class of Reducer
		Job5.setOutputValueClass(FloatWritable.class);

		FileInputFormat.setInputPaths(Job5, new Path(inputPath));
		FileOutputFormat.setOutputPath(Job5, new Path(outputPath));

		Job5.setInputFormatClass(TextInputFormat.class);
		Job5.setOutputFormatClass(TextOutputFormat.class);

		Job5.setMapperClass(mapperJob5.class);

		Job5.setNumReduceTasks(1);   				//To enforce a single reducer for comparison purposes
		Job5.setReducerClass(reducerJob5.class);

		boolean success = Job5.waitForCompletion(true);
		
		Path srcPath = new Path(outputPath);
		Path dstPath = new Path(CopyPath);
		FileSystem fs_src = srcPath.getFileSystem(conf);
		FileSystem fs_dst = dstPath.getFileSystem(conf);
		FileUtil.copyMerge(fs_src, srcPath, fs_dst, dstPath, true, conf, null);
		
		return success;
	}
    
}
