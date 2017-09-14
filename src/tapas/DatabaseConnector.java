package tapas;

import java.io.FileNotFoundException;
import java.io.IOException;
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

import jgibblda.LDACmdOption;

public class DatabaseConnector
{
	private Connection conn;
	private TopicModel topicModel;
	private LDACmdOption option;
	/**
	 * Contains map for translation between corpus facet's (~ labels') ID in DB and the local one used for ingestion of data into LLDA.
	 * Static, since other instances access the translation too.
	 */
	public static Map<Integer, Integer> corpusFacetIDs_globalToLocal;
	public static Map<Integer, Integer> corpusFacetIDs_localToGlobal;


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


    	DatabaseConnector.corpusFacetIDs_globalToLocal = new HashMap<Integer, Integer>();
    	DatabaseConnector.corpusFacetIDs_localToGlobal = new HashMap<Integer, Integer>();
    	// Extract topic model from DB.
    	extractTopicModel(Integer.parseInt(option.db_topic_model_id));
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
			    								rs.getInt("corpora_id"),
			    								rs.getInt("corpus_features_id"));
			}

			rs.close();
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return topicModel;
	}

	/**
	 * Returns all documents in corpus in requested format, i. e. ([label1, ..., labeln] document_text)
	 * @param corpusID
	 * @return
	 */
	public String[] loadLabeledDocumentsInCorpus(int corpusID)
	{
		PreparedStatement st;
		ResultSet rs;
		String[] labeledDocuments = null;

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
			// Initialize string array for documents with number of records.
			labeledDocuments = new String[numberOfDocuments];


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
					+ "	cfe.title != 'document_id'"
					+ "WHERE "
					+ "	d.corpora_id = ?"
					// Don't use document IDs as facets (for summarization? There are other methods; e. g. inferring doc. if really necessary).
					+ ""
					+ ""
					+ "group by "
					+ "d.id, d.refined_text");

			st.setInt(1, corpusID);
			rs = st.executeQuery();

			// 3. Prepare array of document string.
			int localFacetIndex = 0;
			int rowIndex = 0;
			while (rs.next()) {
				// Translate document facet DB IDs to local ones in range [0, k - 1], where k is number of topics (equals number of facets).
				// See https://github.com/myleott/JGibbLabeledLDA - required by library for whatever reason.
				Array rsArray = rs.getArray("corpus_facet_ids_in_document");
				Integer[] corpusFacetIDsInDocument = (Integer[])rsArray.getArray();
				// String representation of corpus facets in document.
				String labeledDocumentString = "[";

				// Build string representation of local facet IDs.
				for (int corpusFacetID : corpusFacetIDsInDocument) {
					if (!DatabaseConnector.corpusFacetIDs_globalToLocal.containsKey(corpusFacetID)) {
						DatabaseConnector.corpusFacetIDs_globalToLocal.put(corpusFacetID, localFacetIndex);
						DatabaseConnector.corpusFacetIDs_localToGlobal.put(localFacetIndex, corpusFacetID);
						localFacetIndex++;
					}

					labeledDocumentString += String.valueOf(DatabaseConnector.corpusFacetIDs_globalToLocal.get(corpusFacetID)) + " ";
				}
				// Remove last whitespace, add closing bracket.
				labeledDocumentString = labeledDocumentString.substring(0, labeledDocumentString.length() - 1) + "]";
				// Add document to labeledDocumentString.
				labeledDocumentString += " " + rs.getString("refined_text");

				// Add labeledDocumentString to result set.
				labeledDocuments[rowIndex++] = labeledDocumentString;
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return labeledDocuments;
	}

	public Map<String, Integer> loadWordToIDMap(int corpusID)
	{
		PreparedStatement st;
		ResultSet rs;
		Map<String, Integer> wordsToIDs = new HashMap<String, Integer>();

		// 2. Load documents and corpus facets. Use the latter for generating array of local label indices.
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
			st.setInt(1, corpusID);
			rs = st.executeQuery();

			while (rs.next()) {
				wordsToIDs.put(rs.getString("term"), rs.getInt("id"));
			}
		}

		catch (SQLException e) {
			e.printStackTrace();
		}

		return wordsToIDs;
	}
}
