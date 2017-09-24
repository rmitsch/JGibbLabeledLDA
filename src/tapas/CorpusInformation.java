package tapas;

import java.util.HashMap;
import java.util.Map;

/**
 * Class containing data and metadata on corpus in database.
 * Purely used as bean.
 * @author raphael
 *
 */
public class CorpusInformation
{
	private String[] labeledDocuments;
	/**
	 * Facets' DB ID -> facet's locally used sequence index/ID.
	 */
	private Map<Integer, Integer> corpusFacetIDs_globalToLocal;
	/**
	 * Facets' locally used sequence index/ID -> facets' DB ID.
	 */
	private Map<Integer, Integer> corpusFacetIDs_localToGlobal;
	/**
	 * Documents' locally used sequence index/ID -> documents' DB ID.
	 */
	private Map<Integer, Integer> documentIDs_localToGlobal;

	public CorpusInformation(int numberOfDocuments)
	{
		this.labeledDocuments 				= new String[numberOfDocuments];
		this.corpusFacetIDs_globalToLocal 	= new HashMap<Integer, Integer>();
		this.corpusFacetIDs_localToGlobal 	= new HashMap<Integer, Integer>();
		this.documentIDs_localToGlobal		= new HashMap<Integer, Integer>();
	}

	public String[] getLabeledDocuments() {
		return labeledDocuments;
	}

	public Map<Integer, Integer> getCorpusFacetIDs_globalToLocal() {
		return corpusFacetIDs_globalToLocal;
	}

	public Map<Integer, Integer> getCorpusFacetIDs_localToGlobal() {
		return corpusFacetIDs_localToGlobal;
	}

	public Map<Integer, Integer> getDocumentIDs_localToGlobal() {
		return documentIDs_localToGlobal;
	}
}
