/*
 * Copyright (C) 2007 by
 *
 * 	Xuan-Hieu Phan
 *	hieuxuan@ecei.tohoku.ac.jp or pxhieu@gmail.com
 * 	Graduate School of Information Sciences
 * 	Tohoku University
 *
 *  Cam-Tu Nguyen
 *  ncamtu@gmail.com
 *  College of Technology
 *  Vietnam National University, Hanoi
 *
 * JGibbsLDA is a free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published
 * by the Free Software Foundation; either version 2 of the License,
 * or (at your option) any later version.
 *
 * JGibbsLDA is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with JGibbsLDA; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.
 */
package jgibblda;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.hash.TIntHashSet;
import tapas.CorpusInformation;
import tapas.DatabaseConnector;
import tapas.TopicModel;

public class LDADataset {
    //---------------------------------------------------------------
    // Instance Variables
    //---------------------------------------------------------------

    public Dictionary localDict = new Dictionary();			// local dictionary
    public ArrayList<Document> docs = new ArrayList<Document>(); 		// a list of documents
    public int M = 0; 			 		// number of documents
    public int V = 0;			 		// number of words

    // map from local coordinates (id) to global ones
    // null if the global dictionary is not set
    public TIntIntHashMap lid2gid = null;

    //link to a global dictionary (optional), null for train data, not null for test data
    public Dictionary globalDict = null;

    // Connection to database.
    DatabaseConnector dbConnector;

	/**
	 * Contains map for translation between corpus facet's (~ labels') ID in DB and the local one used for ingestion of data into LLDA.
	 * Static, since other instances access the translation too.
	 */
	public Map<Integer, Integer> corpusFacetIDs_globalToLocal;
	public Map<Integer, Integer> corpusFacetIDs_localToGlobal;

	/**
	 * Map for translating words to IDs (in DB table for terms_in_corpora).
	 */
	public Map<String, Integer> wordsToDBIDs;

    //-------------------------------------------------------------
    //Public Instance Methods
    //-------------------------------------------------------------

    public LDADataset(LDACmdOption option, boolean readCorpus)
    {
    	this.dbConnector = new DatabaseConnector(option);

    	// Initialize maps for ID translation.
    	wordsToDBIDs 					= new HashMap<String, Integer>();

    	if (readCorpus) {
    		// Fetch topic model.
        	TopicModel topicModel = dbConnector.extractTopicModel(Integer.parseInt(option.db_topic_model_id));

    		// Read data set in specified database.
    		try {
    			// Read data set.
				readDataSet(topicModel.getCorpora_id(), option, option.unlabeled);

				// Update option by deriving implicit parameter.
				option.deriveImplicitSettings(topicModel, corpusFacetIDs_globalToLocal.size());

				 // Load map of words in DB to IDs.
	            this.wordsToDBIDs = dbConnector.loadWordToIDMap(option);

		    	// Close database connection.
		    	this.dbConnector.getConnection().close();
			}

    		catch (FileNotFoundException e) {
				e.printStackTrace();
			}

    		catch (SQLException e) {
				e.printStackTrace();
			}

    		catch (IOException e) {
				e.printStackTrace();
			}
    	}
    }


    public void setM(int M)
    {
        this.M = M;
    }

    public void setDictionary(Dictionary globalDict)
    {
        lid2gid = new TIntIntHashMap();
        this.globalDict = globalDict;
    }

    /**
     * set the document at the index idx if idx is greater than 0 and less than M
     * @param doc document to be set
     * @param idx index in the document array
     */
    public void setDoc(Document doc, int idx){
        if (idx < docs.size()) {
            docs.set(idx, doc);
        } else {
            docs.add(idx, doc);
        }
    }

    /**
     * add a new document
     * @param str string contains doc
     */
    public void addDoc(String str, boolean unlabeled)
    {
        // read document labels (if provided)
        TIntArrayList labels = null;
        if (str.startsWith("[")) {
            String[] labelsBoundary = str.
                substring(1). // remove initial '['
                split("]", 2); // separate labels and str between ']'
            String[] labelStrs = labelsBoundary[0].trim().split("[ \\t]");
            str = labelsBoundary[1].trim();

            // parse labels (unless we're ignoring the labels)
            if (!unlabeled) {
                // store labels in a HashSet to ensure uniqueness

                TIntHashSet label_set = new TIntHashSet();
                for (String labelStr : labelStrs) {
                    try {
                        label_set.add(Integer.parseInt(labelStr.trim()));
                    } catch (NumberFormatException nfe) {
                        System.err.println("Unknown document label ( " + labelStr + " ) for document " + docs.size() + ".");
                    }
                }
                labels = new TIntArrayList(label_set);
                labels.sort();
            }
        }

        String[] words = str.split("[ \\t\\n]");
        TIntArrayList ids = new TIntArrayList();
        for (String word : words){
            if (word.trim().equals("")) {
                continue;
            }

            int _id = localDict.word2id.size();

            if (localDict.contains(word))
                _id = localDict.getID(word);

            if (globalDict != null) {
                //get the global id
                if (globalDict.contains(word)) {
                    localDict.addWord(word);

                    lid2gid.put(_id, globalDict.getID(word));
                    ids.add(_id);
                }
            }
            else {
                localDict.addWord(word);
                ids.add(_id);
            }
        }

        setDoc(new Document(ids, str, labels), docs.size());

        V = localDict.word2id.size();
    }

    //---------------------------------------------------------------
    // I/O methods
    //---------------------------------------------------------------

    /**
     * read a dataset from a file
     * @return true if success and false otherwise
     */
    public boolean readDataSet(String filename, boolean unlabeled) throws FileNotFoundException, IOException
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(
                        new FileInputStream(filename)), "UTF-8"));
        try {
            String line;
            // Here: Get all documents from database, iterate over them. Add int. representation of labels before start of document.
            while ((line = reader.readLine()) != null) {
            	// Here
                addDoc(line, unlabeled);
            }
            setM(docs.size());

            // debug output
            System.out.println("Dataset loaded:");
            System.out.println("\tM:" + M);
            System.out.println("\tV:" + V);

            return true;
        } finally {
            reader.close();
        }
    }

    /**
     * Read corpus from TAPAS database.
     * @param corpusID
     * @param option
     * @param unlabeled
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    public boolean readDataSet(int corpusID, LDACmdOption option, boolean unlabeled) throws FileNotFoundException, IOException
    {
    	// Fetch corpus information record.
    	CorpusInformation corpusInfo = dbConnector.loadLabeledDocumentsInCorpus(option, corpusID);
    	// Copy reference to dictionaries.
    	this.corpusFacetIDs_globalToLocal = corpusInfo.getCorpusFacetIDs_globalToLocal();
    	this.corpusFacetIDs_localToGlobal = corpusInfo.getCorpusFacetIDs_localToGlobal();

    	// Add documents.
    	for (String labeledDocument : corpusInfo.getLabeledDocuments()) {
    		addDoc(labeledDocument, unlabeled);
    	}
        setM(docs.size());

        // debug output
        System.out.println("Dataset loaded:");
        System.out.println("\tM:" + M);
        System.out.println("\tV:" + V);

        return true;
    }


	public Map<Integer, Integer> getCorpusFacetIDs_globalToLocal()
	{
		return corpusFacetIDs_globalToLocal;
	}

	public Map<Integer, Integer> getCorpusFacetIDs_localToGlobal()
	{
		return corpusFacetIDs_localToGlobal;
	}
}
