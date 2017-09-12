package tapas;

import jgibblda.LDACmdOption;

public class TopicModel
{
	private int ID;
	private double alpha;
	private int kappa;
	private int n_iterations;
	private int corpora_id;
	private int corpus_features_id;

	public TopicModel()
    {
    }

	public int getID() {
		return ID;
	}

	public double getAlpha() {
		return alpha;
	}

	public int getKappa() {
		return kappa;
	}

	public int getN_iterations() {
		return n_iterations;
	}

	public int getCorpora_id() {
		return corpora_id;
	}

	public int getCorpus_features_id() {
		return corpus_features_id;
	}
}
