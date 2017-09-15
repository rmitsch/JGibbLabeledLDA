package tapas;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.zip.GZIPOutputStream;

import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.sun.webkit.graphics.Ref;

import jgibblda.LDACmdOption;
import jgibblda.LDADataset;

public class DatabaseConnector
{
	private Connection conn;

	/**
	 * Create new instance of database connector.
	 * @param option
	 */
	public DatabaseConnector(LDACmdOption option)
    {
		String url = 	"jdbc:postgresql://" +
    					option.db_host +
    					":" + option.db_port +
    					"/" + option.db_database +
    					"?user=" + option.db_user +
    					"&password=" + option.db_password;

    	try {
			this.conn = DriverManager.getConnection(url);
		}

    	catch (SQLException e) {
			System.out.println("Failed connection to db with connection URL " + url + ".");
		}
    }

	/**
	 * Extracts topic model from database.
	 * @return Topic model with specified ID.
	 */
	public TopicModel extractTopicModel(int topicModelID)
	{
		Statement sql_stmt;
		TopicModel topicModel = null;

		try {
			PreparedStatement st = conn.prepareStatement("SELECT * FROM topac.topic_models WHERE id = ?");
			st.setInt(1, topicModelID);

			ResultSet rs = st.executeQuery();

			// Read topic model.
			if (rs.next())
			{
			    topicModel = new TopicModel(	rs.getInt("id"),
			    								rs.getDouble("alpha"),
			    								rs.getDouble("eta"),
			    								rs.getInt("kappa"),
			    								rs.getInt("n_iterations"),
			    								rs.getInt("corpora_id"));
			}

			rs.close();
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return topicModel;
	}

	public Connection getConnection()
	{
		return this.conn;
	}

	/**
	 * Returns all documents in corpus in requested format, i. e. ([label1, ..., labeln] document_text).
	 * Also generates dictionaries translating between local and global (e. g. in database) indices for corpus facets.
	 * @param option
	 * @param corpusID
	 * @return
	 */
	public CorpusInformation loadLabeledDocumentsInCorpus(LDACmdOption option, int corpusID)
	{
		PreparedStatement st;
		ResultSet rs;
		CorpusInformation corpusInfo = null;

		try {
			// 1. Get number of documents in corpus.
			st = conn.prepareStatement(
					"SELECT count(*) as doc_count FROM topac.documents d "
					+ "WHERE d.corpora_id = ? ");
			st.setInt(1, corpusID);
			rs = st.executeQuery();
			int numberOfDocuments = 0;
			if (rs.next()) {
				numberOfDocuments = rs.getInt("doc_count");
			}
			// Initialize container object for corpus information.
			corpusInfo = new CorpusInformation(numberOfDocuments);


			// 2. Load documents and corpus facets. Use the latter for generating array of local label indices.
			st = conn.prepareStatement(
					"SELECT d.id, d.refined_text, array_agg(cf.id) corpus_facet_ids_in_document "
					+ "FROM topac.documents d "
					+ "inner join topac.corpus_features_in_documents cfid on "
					+ "	cfid.documents_id = d.id "
					+ "inner join topac.corpus_facets cf on "
					+ "cf.corpus_features_id = cfid.corpus_features_id and "
					+ "cf.corpus_feature_value = cfid.value "
					+ "inner join topac.corpus_features cfe on "
					+ "	cfe.id = cf.corpus_features_id and"
					// Don't use document IDs as facets (for summarization? There are other methods; e. g. inferring doc. if really necessary).
					+ "	cfe.title != 'document_id'"
					+ "WHERE "
					+ "	d.corpora_id = ?"
					+ ""
					+ ""
					+ "group by "
					+ "d.id, d.refined_text");

			st.setInt(1, corpusID);
			rs = st.executeQuery();

			// 3. Prepare array of document string.
			int localFacetIndex = 0;
			int rowIndex = 0;
			// Fetch collection of documents.
			String[] labeledDocuments = corpusInfo.getLabeledDocuments();
			// Fetch translation dictionaries.
			Map<Integer, Integer> corpusFacetIDs_globalToLocal = corpusInfo.getCorpusFacetIDs_globalToLocal();
			Map<Integer, Integer> corpusFacetIDs_localToGlobal = corpusInfo.getCorpusFacetIDs_localToGlobal();

			// Iterate over result set (documents + labels).
			while (rs.next()) {
				// Translate document facet DB IDs to local ones in range [0, k - 1], where k is number of topics (equals number of facets).
				// See https://github.com/myleott/JGibbLabeledLDA - required by library for whatever reason.
				Array rsArray = rs.getArray("corpus_facet_ids_in_document");
				Integer[] corpusFacetIDsInDocument = (Integer[])rsArray.getArray();
				// String representation of corpus facets in document.
				String labeledDocumentString = "[";

				// Build string representation of local facet IDs.
				for (int corpusFacetID : corpusFacetIDsInDocument) {
					if (!corpusFacetIDs_globalToLocal.containsKey(corpusFacetID)) {
						corpusFacetIDs_globalToLocal.put(corpusFacetID, localFacetIndex);
						corpusFacetIDs_localToGlobal.put(localFacetIndex, corpusFacetID);
						localFacetIndex++;
					}
					// Append localized facet ID to document string.
					labeledDocumentString += String.valueOf(corpusFacetIDs_globalToLocal.get(corpusFacetID)) + " ";
				}
				// Remove last whitespace, add closing bracket.
				labeledDocumentString = labeledDocumentString.substring(0, labeledDocumentString.length() - 1) + "]";
				// Add document to labeledDocumentString.
				labeledDocumentString += " " + rs.getString("refined_text");

				// Add labeledDocumentString to result set.
				labeledDocuments[rowIndex++] = labeledDocumentString;
			}
		}

		catch (SQLException e) {
			e.printStackTrace();
		}

		return corpusInfo;
	}

	/**
	 * Loads map translating words into IDs for table terms_in_corpora.
	 * @param corpusID
	 * @return
	 */
	public Map<String, Integer> loadWordToIDMap(LDACmdOption option)
	{
		PreparedStatement st;
		ResultSet rs;
		Map<String, Integer> wordsToDBIDs = new HashMap<String, Integer>();

		try {
			st = conn.prepareStatement(
					"select "
					+ "  t.term, "
					+ "  tic.id "
					+ "from "
					+ "  topac.terms_in_corpora tic "
					+ "inner join topac.terms t on "
					+ "  t.id = tic.terms_id "
					+ "where tic.corpora_id = ?");
			st.setInt(1, option.corpusID);
			rs = st.executeQuery();

			while (rs.next()) {
				wordsToDBIDs.put(rs.getString("term"), rs.getInt("id"));
			}
		}

		catch (SQLException e) {
			e.printStackTrace();
		}

		return wordsToDBIDs;
	}

	/**
	 * Stores topic entries in DB. Used key in provided map to refer to facets in DB.
	 * Topics' sequence number can be inferred - lookup with algorithm works using facet ID translation.
	 * @param corpusFacetIDs_globalToLocal
	 * @param option
	 * @return facetDBIDs_to_topicDBIDs Map linking DB IDs of facets to DB IDs of topics.
	 */
	public Map<Integer, Integer> saveTopicsForFacets(Map<Integer, Integer> corpusFacetIDs_globalToLocal, LDACmdOption option)
	{
		PreparedStatement st;
		ResultSet rs;
		Map<Integer, Integer> facetDBIDs_to_topicDBIDs = new HashMap<Integer, Integer>();

		try {
			// Make sure autocommit is disabled.
			conn.setAutoCommit(false);

			// Prepare statement for insertion of topics.
			st =  conn.prepareStatement(
					"insert into topac.topics ("
					+ "	sequence_number, "
					+ " title, "
					+ " topic_models_id, "
					+ " quality, "
					+ "	coordinates, "
					+ "	coherence, "
					+ "	corpus_facets_id"
					+ ") "
					+ "values (?, ?, ?, ?, ? ,?, ?)" + "",
					// Make sure we fetch the returned topic IDs.
					new String[] {"id", "corpus_facets_id"}
			);

			// Iterate over map of facet IDs. Key is global/DB ID, value is corresponding local index.
			// Create one topic for each facet (since this is the result of a LLDA computation).
			int sequence_number = 0;
			for (Map.Entry<Integer, Integer> facetIDs : corpusFacetIDs_globalToLocal.entrySet()) {
				st.setInt(1, sequence_number++);
				st.setString(2, "");
				st.setInt(3, Integer.parseInt(option.db_topic_model_id));
				st.setFloat(4, -1);

				String[] coordinates = { "-1" };
				st.setArray(5, conn.createArrayOf("FLOAT", coordinates));

				st.setFloat(6, -1);
				st.setInt(7, facetIDs.getKey());

				// Add topic to  batch.
				st.addBatch();
			}
			// Execute bulk insert.
			st.executeBatch();
			conn.commit();
			// Fetch generated keys.
			rs = st.getGeneratedKeys();

			// Generate map for translating facet DB ID to topic DB ID.
			while (rs.next()) {
				facetDBIDs_to_topicDBIDs.put(rs.getInt("corpus_facets_id"), rs.getInt("id"));
			}

			st.close();
		}

		catch (SQLException e) {
			e.printStackTrace();
		}

		return facetDBIDs_to_topicDBIDs;
	}


	/**
	 * Save word-in-topic probabilities.
	 * @param K
	 * @param V
	 * @param data
	 * @param phi
	 * @param facetDBIDs_to_topicDBIDs
	 */
	public void saveWordInTopicsProbabilities(	int K,
												int V,
												LDADataset data,
												double[][] phi,
												Map<Integer, Integer> facetDBIDs_to_topicDBIDs)
	{
		PreparedStatement st;
		ResultSet rs;

		try {
			// Make sure autocommit is disabled.
			conn.setAutoCommit(false);

			// Drop foreign key constraints temporarily for speedup (guaranteed by earlier loading from DB, meanwhile no write actions
			// since this is a single-user system.
			st = conn.prepareStatement("alter table topac.terms_in_topics drop constraint terms_in_topics_terms_in_corpora");
			st.execute();
			// Disable synchronous commits for transactions.
			st = conn.prepareStatement("set synchronous_commit to off");
			st.execute();
			conn.commit();

			// Define batch size to reduce memory footprint.
			final int batchSize = 5000000;
			int count = 0;

			CopyManager copyManager = new CopyManager((BaseConnection) conn);
			CopyIn copyIn = copyManager.copyIn("COPY topac.terms_in_topics FROM STDIN WITH DELIMITER ','");

			// Iterate over topics.
			for (int i = 0; i < K; i++) {
				// Fetch corresponding ID of facet in DB.
				int facetID = data.corpusFacetIDs_localToGlobal.get(i);
				// Fetch ID of corresponding topic in DB.
				int topicID = facetDBIDs_to_topicDBIDs.get(facetID);

				// Iterate over words.
	            for (int j = 0; j < V; j++) {
	            	byte[] bytesToAppend = (topicID + "," + data.wordsToDBIDs.get(data.localDict.getWord(j)) + "," + (float)phi[i][j] + "\n").getBytes();
	                copyIn.writeToCopy(bytesToAppend, 0, bytesToAppend.length);

		            // Execute batch if batch size is reached.
		            if(++count % batchSize == 0) {
		            	System.out.println("Executing batch");
		            	copyIn.endCopy();
		            	conn.commit();
		            	// Prepare copyIn instance for next batch.
		            	copyIn = copyManager.copyIn("COPY topac.terms_in_topics FROM STDIN WITH DELIMITER ','");
		            }
	            }
	        }

			// Insert remaining terms_in_topics.
			st.executeBatch();
			conn.commit();

			// Reintroduce foreign key constraint.
			st = conn.prepareStatement(
					"ALTER TABLE topac.terms_in_topics ADD CONSTRAINT terms_in_topics_terms_in_corpora "
				    + "FOREIGN KEY (terms_in_corpora_id) "
				    + "REFERENCES topac.terms_in_corpora (id) "
				    + "NOT DEFERRABLE "
				    + "INITIALLY IMMEDIATE");
			st.execute();
			// Disable synchronous commits for transactions.
			st = conn.prepareStatement("set synchronous_commit to on");
			st.execute();
			conn.commit();

			st.close();
		}

		catch (SQLException e) {
			e.printStackTrace();
		}

	}
}
