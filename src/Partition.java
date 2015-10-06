package MarketRatingsPkg;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/*
 * Partitions the output data into
 * Six different files:
 * 						1 partition is all ratings below 50
 * 						1 partition is all ratings 50 and above and below 60
 * 						etc etc etc
 */


public class Partition extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		
		String[] string2 = value.toString().split("\t");
		
		if (Integer.parseInt(string2[2]) < 50){
			return 0;
		}
		else if (Integer.parseInt(string2[2]) >= 50 && Integer.parseInt(string2[2]) < 60){
			return 1 % numReduceTasks;
		}
		else if (Integer.parseInt(string2[2]) >= 60 && Integer.parseInt(string2[2]) < 70){
			return 2 % numReduceTasks;
		}
		else if (Integer.parseInt(string2[2]) >= 70 && Integer.parseInt(string2[2]) < 80){
			return 3 % numReduceTasks;
		}
		else if (Integer.parseInt(string2[2]) >= 80 && Integer.parseInt(string2[2]) < 90){
			return 4 % numReduceTasks;
		}
		else if (Integer.parseInt(string2[2]) >= 90 && Integer.parseInt(string2[2]) <=100){
			return 5 % numReduceTasks;
		}
		else
			return 0;
		
	}

}
