
import java.io.IOException;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Note: We are using 0 reducer:
//in this case we will get 'pure' output from the mappers without any further processing
//sorting/shuffling in reducer

public class mapperJob5 extends Mapper<LongWritable, Text, FloatWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String numberofnodes = conf.get("NumberOfNodes");
		float N = Float.valueOf(numberofnodes);
		
		String[] pageAndRank = getPageAndRank(key, value);

		float parseFloat = Float.parseFloat(pageAndRank[1]);
		float threshold = 5.0f / N;
		if (parseFloat >= threshold ){          

			Text page = new Text(pageAndRank[0]);
			FloatWritable rank = new FloatWritable(parseFloat);

			context.write(rank,page);
		}
	}
	private String[] getPageAndRank(LongWritable key, Text value) throws CharacterCodingException {
		String[] pageAndRank = new String[2];
		int tabPageIndex = value.find("@");
		int tabRankIndex = value.find("@", tabPageIndex + 1);

		// no tab after rank (when there are no links)
		int end;
		if (tabRankIndex == -1) {
			end = value.getLength() - (tabPageIndex + 1);
		} else {
			end = tabRankIndex - (tabPageIndex + 1);
		}

		pageAndRank[0] = Text.decode(value.getBytes(), 0, tabPageIndex);
		pageAndRank[1] = Text.decode(value.getBytes(), tabPageIndex + 1, end);

		return pageAndRank;
	}
}

