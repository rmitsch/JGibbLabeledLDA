package tapas;

import jgibblda.LDACmdOption;

public class TopicModel
{
	private int ID;
	private double alpha;
	private double eta;
	private int kappa;
	private int n_iterations;
	private int corpora_id;
	private int corpus_features_id;

	public TopicModel(int ID, double alpha, double eta, int kappa, int n_iterations, int corpora_id, int corpus_features_id)
    {
		this.ID = ID;
		this.alpha = alpha;
		this.eta = eta;
		this.kappa = kappa;
		this.n_iterations = n_iterations;
		this.corpora_id = corpora_id;
		this.corpus_features_id = corpus_features_id;
    }

	public int getID() {
		return ID;
	}

	public double getAlpha() {
		return alpha;
	}

	public double getEta() {
		return eta;
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
