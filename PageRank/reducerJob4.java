import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class reducerJob4 extends Reducer<Text, Text, Text, Text> {

		private static final float damping = 0.85F;

		@Override
		public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//Get number of nodes:
			Configuration conf = context.getConfiguration();
			String numberofnodes = conf.get("NumberOfNodes");
			float N = Float.valueOf(numberofnodes);

			boolean isExistingWikiPage = false;
			String[] split;
			float sumShareOtherPageRanks = 0.0f;
			String links = "";
			String pageWithRank;

			for (Text value : values){
				pageWithRank = value.toString();

				if(pageWithRank.equals("!")) {
					isExistingWikiPage = true;
					continue;
				}

				if(pageWithRank.startsWith("*")){  //The node doesn't count in the summation since it doesn't have an outlink
					links = "@"+pageWithRank.substring(1);
					continue;
				}

				split = pageWithRank.split("@");
				//split[0] is the list of outlinks
				float pageRank = Float.valueOf(split[1]); //split[1] is current rank
				int countOutLinks = Integer.valueOf(split[2]); //split[2] is number of outlinks

				sumShareOtherPageRanks += (pageRank/countOutLinks);
			}

			float newRank = damping * sumShareOtherPageRanks + (1-damping)/N;

			if(!isExistingWikiPage) return;
			context.write(page, new Text(newRank + links));
		}
	}
