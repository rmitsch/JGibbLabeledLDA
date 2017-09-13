package jgibblda;

import org.kohsuke.args4j.*;

import tapas.TopicModel;

public class LDACmdOption {

    @Option(name="-est", usage="Specify whether we want to estimate model from scratch")
        public boolean est = false;

    @Option(name="-estc", usage="Specify whether we want to continue the last estimation")
        public boolean estc = false;

    @Option(name="-inf", usage="Specify whether we want to do inference")
        public boolean inf = true;

    @Option(name="-infseparately", usage="Do inference for each document separately")
        public boolean infSeparately = false;

    @Option(name="-unlabeled", usage="Ignore document labels")
        public boolean unlabeled = false;

    @Option(name="-nburnin", usage="Specify the number of burn-in iterations")
        public int nburnin = 500;

    @Option(name="-samplinglag", usage="Specify the sampling lag")
        public int samplingLag = 5;

    @Option(name="-db_host", usage="Specify the database host")
    	public String db_host = "";

    @Option(name="-db_port", usage="Specify the database port")
		public String db_port = "";

    @Option(name="-db_database", usage="Specify the database")
		public String db_database = "";

    @Option(name="-db_user", usage="Specify the database user")
		public String db_user = "";

    @Option(name="-db_password", usage="Specify the database password")
		public String db_password = "";

    @Option(name="-db_topic_model_id", usage="Specify the database ID of topic model")
		public String db_topic_model_id = "";

    // ----------------------------------
    // Legacy options, no longer used.
    // Kept here for readability.
    // ----------------------------------

    public String dir = "";
    public String dfile = "";
    public String modelName = "";
    public int twords = 0;

    // ----------------------------------
    // From here: derived options.
    // ----------------------------------

    public double alpha = -1;
    public double beta = -1;
    public int niters = 1000;
    public int K = 100;

    // ----------------------------------
    // From here: Methods.
    // ----------------------------------

    /**
     * Derives implicit settings by extracting data from loaded dataset.
     * @param topicModel
     * @param numberOfFacets Number of feature values in this corpus.
     */
    public void deriveImplicitSettings(TopicModel topicModel, int numberOfFacets)
    {
    	this.alpha = topicModel.getAlpha();
    	// Is beta different nomenclature for eta?
    	this.beta = topicModel.getEta();
    	this.niters = topicModel.getN_iterations();
    	// Number of topics has to equal to the number of facets.
    	this.K = numberOfFacets;
    }

}
