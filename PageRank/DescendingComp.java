
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingComp extends WritableComparator {
	protected DescendingComp() {
		super(FloatWritable.class, true);
	}
	//To make the ascending sort to descending sort,
	// we multiply by -1,
	//So if a and b are positive and a>b then
	// if -a < -b, therefore sorting in ascending order of -a and -b
	// is equivalent with descending for a and b
	public int compare(WritableComparable w1, WritableComparable w2) {
		FloatWritable k1 = (FloatWritable)w1;
		FloatWritable k2 = (FloatWritable)w2;
		
		return -1 * k1.compareTo(k2); //returns 1 if k1<k2
	}
}