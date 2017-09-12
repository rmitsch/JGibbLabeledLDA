package tapas;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import jgibblda.LDACmdOption;

public class DatabaseConnector
{
	private Connection conn;
	private TopicModel topicModel;
	private LDACmdOption option;

	public DatabaseConnector(LDACmdOption option)
    {
		this.option = option;
    	String url = 	"jdbc:postgresql://" +
    					option.db_host +
    					":" + option.db_port +
    					"/" + option.db_database +
    					"?user=" + option.db_user +
    					"&password=" + option.db_password;

    	try {
			this.conn = DriverManager.getConnection(url);
		} catch (SQLException e) {
			System.out.println("Failed connection to db with connection URL " + url + ".");
		}

    	// Extract topic model from DB.
    	extractTopicModel();
    }

	private void extractTopicModel()
	{
		Statement sql_stmt;
		try {
			PreparedStatement st = conn.prepareStatement("SELECT * FROM topac.topic_models WHERE id = ?");
			st.setInt(1, Integer.parseInt(option.db_topic_model_id));
			ResultSet rs = st.executeQuery();

//			Next up:
//				- Build topic model from DB result.
//				- Modify LDADataset() in order to fetch data from DB.
//				- Check if algorithm runs and produces correctly formatted and reasonable (e.g. words; number of topics and labels etc. is correct).
//				- Modify DB, add word-in-topic probabilities.
//				- Change output methods (Estimator.saveModel()) in order to write to DB.

			while (rs.next())
			{
			    System.out.println(rs.getString(1));
			}

			rs.close();
			st.close();

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
