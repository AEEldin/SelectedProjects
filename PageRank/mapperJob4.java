import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class mapperJob4 extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int pageTabIndex = value.find("@");  //find index of first & 
			int rankTabIndex = value.find("@", pageTabIndex+1);  //find next & by searching for first starting from position pageTabIndex+1
			//Make sure there exists PageName
			if(pageTabIndex == -1) return;
			if(rankTabIndex == -1) return;


			String page = Text.decode(value.getBytes(), 0, pageTabIndex);
			String pageWithRank = Text.decode(value.getBytes(), 0, rankTabIndex+1);

			// Mark page as an Existing page (ignore red wiki-links)
			context.write(new Text(page), new Text("!"));


			String links = Text.decode(value.getBytes(), rankTabIndex+1, value.getLength()-(rankTabIndex+1));
			String[] allOtherPages = links.split("\t"); //the separator between outlinks
			int totalLinks = allOtherPages.length;

			for (String otherPage : allOtherPages){
				Text pageRankTotalLinks = new Text(pageWithRank + totalLinks);
				context.write(new Text(otherPage), pageRankTotalLinks);
			}

			// Put the original links of the page for the reduce output
			context.write(new Text(page), new Text("*" + links));  //emit a dummy character for those with no outlink
		}
	}

