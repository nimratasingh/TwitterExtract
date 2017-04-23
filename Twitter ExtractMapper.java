import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;

/** To extract tweets from a data file in JSON format


public class TwitterExtractMapper extends Mapper<LongWritable, Text, Text,Text> {


	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
		String tagged = null;


		String jsonString = value.toString().trim();
		Gson gson = new Gson();
		//		Object gsonContent = gson.fromJson(jsonString,JsonObject.class);
		try{
			
			JsonObject jsonObject = gson.fromJson(jsonString, JsonObject.class);
			
			if(jsonObject.has("created_at")){
				String id_str = jsonObject.get("id_str").getAsString();		
				String created_at = jsonObject.get("created_at").getAsString();
				String text = jsonObject.get("text").getAsString();

				if(jsonObject.get("lang")!= null){

					String lang = jsonObject.get("lang").getAsString();

					if(lang.equalsIgnoreCase("en")){

						String v = text+"\t"+lang;

						context.write(new Text ("id: "+id_str+"\t"+"created_at: "+ created_at), new Text(v) );		
					}
				}
			}
		}
		catch(JsonSyntaxException e){
			 e.printStackTrace();

		}

	}
}
