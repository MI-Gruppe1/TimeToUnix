import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import com.google.gson.Gson;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;


public class ConvertTimeToUnix {

	private static Connection connection;

	private static String changeTimeStamptoUnixTimeString(String str) throws ParseException {
		// 2016-11-09T13:04:53.973000Z
		String[] timewithoutmillis = str.split("\\.");
		String timeStamp = timewithoutmillis[0] + "Z";

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		Date date = sdf.parse(timeStamp);
		long milis = date.getTime() / 1000;
		return String.valueOf(milis);
	}

	private static Connection connectToDB() {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			connection = (Connection) DriverManager.getConnection("jdbc:mysql://localhost/mi", "mi", "miws16");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return connection;
	}

	private static class DataObject {

		private String id;
		private String name;
		private double latitude;
		private double longitude;
		private int freebikes;
		private String timestamp;

		public DataObject(String idd, String stname, int frbi, String time, double lat, double lng) {
			
			id = idd;
			name = stname;
			freebikes = frbi;
			timestamp = time;
			latitude = lat;
			longitude = lng;
			
		}
	
	
	public static void main(String[] args) throws SQLException, ParseException, UnirestException {
		ConvertTimeToUnix obj = new ConvertTimeToUnix();
		obj.connectToDB();
		
		String query = "SELECT * FROM `crawledData`;";

		java.sql.Statement st = connection.createStatement();
		ResultSet rs = st.executeQuery(query);

		ArrayList<DataObject> objList = new ArrayList<DataObject>();

		while (rs.next()) {
			objList.add(new DataObject(rs.getString("station_id"), rs.getString("station_name"), rs.getInt("free_bikes"), rs.getString("information_timestamp"), rs.getDouble("latitude"), rs.getDouble("longitude")));
		}

		st.close();
		
		for (DataObject dataObject : objList) {
		//	System.out.println(dataObject.id + " " + dataObject.name +" " +  Integer.toString(dataObject.freebikes) +" " +  dataObject.timestamp +" " +  Double.toString(dataObject.latitude) +" " +  Double.toString(dataObject.longitude));
			dataObject.timestamp = changeTimeStamptoUnixTimeString(dataObject.timestamp);
			//System.out.println(dataObject.id + " " + dataObject.name +" " +  Integer.toString(dataObject.freebikes) +" " +  dataObject.timestamp +" " +  Double.toString(dataObject.latitude) +" " +  Double.toString(dataObject.longitude));
		}
 
		
	ArrayList<HashMap<String, String>> dataSet = new ArrayList<>();
		
		for (int i = 0; i < objList.size(); i++) {
			HashMap<String, String> jsonObject = new HashMap<String, String>();
			jsonObject.put("id" ,objList.get(i).id);
			jsonObject.put("name", objList.get(i).name);
			jsonObject.put("free_bikes", Integer.toString(objList.get(i).freebikes));
			jsonObject.put("timestamp", objList.get(i).timestamp);
			jsonObject.put("latitude", Double.toString(objList.get(i).latitude));
			jsonObject.put("longitude", Double.toString(objList.get(i).longitude));

			dataSet.add(jsonObject);
		}
		
			
		// Send whole data to DBService
		Unirest.post("http://localhost:4567/newData")
		  .body(new Gson().toJson(dataSet))
		  .asString();
		

	}

 }
}
