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
	private Map<Integer, Integer> corpusFacetIDs_globalToLocal;
	private Map<Integer, Integer> corpusFacetIDs_localToGlobal;

	public CorpusInformation(int numberOfDocuments)
	{
		this.labeledDocuments 				= new String[numberOfDocuments];
		this.corpusFacetIDs_globalToLocal 	= new HashMap<Integer, Integer>();
		this.corpusFacetIDs_localToGlobal 	= new HashMap<Integer, Integer>();
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

}
