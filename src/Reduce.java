package MarketRatingsPkg;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text> {
	
	private boolean hasNumbers = false;
	private boolean isURL = false;
	
	public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException{
		
		int rating = 0;
		int numRated = 0;
		int numTotal = 0;
		
		while (values.iterator().hasNext()){														//iterates through each instance of values
			
			String tokens[] = (values.iterator().next().toString()).split("\t");					//splits each instances of values by tabs
			int total = Integer.parseInt(tokens[0]);												 //
			int numOfMarkets = Integer.parseInt(tokens[1]);											//stores each split into integers
			int ratingVal = Integer.parseInt(tokens[2]);											//
			
			if(ratingVal > 0){																		//ignores rows with no data
				rating = (rating*numRated + ratingVal*numOfMarkets)/(numRated+numOfMarkets);		//updates the rating for that market
				numRated += numOfMarkets;															//updates the number of rated markets
			}			
			
			numTotal += total;																		//updates total number of Markets
		}
						
																							
		char[] cityStateLetters = (key.toString()).toCharArray();									//		
																									//		
		checkNumbers(cityStateLetters);//1															//This set of code is used to exclude data		
																									//with null city and state fields		
		checkURL(key.toString());//2																//
				
																									
		if (rating > 0 && !hasNumbers && !isURL){													//ignores empty data, addresses and URLS
			context.write(key, new Text (numTotal + "\t" + numRated + "\t" + rating));				//writes the "key" and "values" values to context		
		}																							
	}
	
	
	
	
	
	/*
	 * METHODS
	 */	
	
	
	//1
	public void checkNumbers(char[] cityStateLetters ){							//because some fields are empty, it reads
		for (char charCityStateLetters : cityStateLetters){						//ADDRESSES 
			if(Character.isDigit(charCityStateLetters)){						//instead of cities
				hasNumbers = true;												//this addresses that issue
				break;
			}
		}		
	}
	
	
	//2
	public void checkURL(String keyCityState){									//because some fields are empty, it reads
		String firstFourLetters = "";											//URLS
																				//instead of cities
		for (int i = 0; i<4 ; i++){												//this addresses that issue
			char letter = (keyCityState.charAt(i));
			firstFourLetters += letter;
		}
		
		if (firstFourLetters.equals("http"))
			isURL = true;		
	}
}
