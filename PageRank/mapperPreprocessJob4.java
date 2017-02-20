import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

	public class mapperPreprocessJob4 extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int pageTabIndex = value.find("\t");  //find index of first tab 
			//Make sure there exists PageName
			if(pageTabIndex == -1) return;

			String page = Text.decode(value.getBytes(), 0, pageTabIndex);
			String links = Text.decode(value.getBytes(), pageTabIndex+1, value.getLength()-(pageTabIndex+1));

			context.write(new Text(page), new Text(links));  
		}
	}