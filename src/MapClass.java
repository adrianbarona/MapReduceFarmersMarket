package MarketRatingsPkg;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapClass extends Mapper<Object, Text, Text, Text> {
	
	
	private Text location = new Text();
	private Text rating = new Text();
	
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{		
		
		String[] row = value.toString().split("\t");			//split columns by each tab
		
		if (row.length > 31){
			
			String city = row[4];								//gets info from the 5th column (city)
			String state = row[6];								//gets info from the 7th column (state)
			
			int count = 0;
			int rated = 0;
			
			for (int col = 12; col < 37; col++){				//data about market rating resides between the
				if(row[col].equals("Y"))							//11th and 36th columns
					count++;									//for each "yes", 1 point is added to its score
			}
			
			count = (count*100)/25;								//data calculations determine overall score for that city
			
			if(count>0)											//ignores rows with no data
				rated = 1;
			
			location.set(city + ", " + state);					//sets the key to "city, state"
			rating.set(1 + "\t" + rated + "\t" + count);		//sets the value to "x		x		x"
			context.write(location, rating);					// ^^
		}		
	}
	
	
	
	
	
	
	

}
