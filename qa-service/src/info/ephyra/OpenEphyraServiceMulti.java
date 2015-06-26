package info.ephyra;

import info.ephyra.answerselection.AnswerSelection;
import info.ephyra.answerselection.filters.AnswerPatternFilter;
import info.ephyra.answerselection.filters.AnswerTypeFilter;
import info.ephyra.answerselection.filters.DuplicateFilter;
import info.ephyra.answerselection.filters.FactoidSubsetFilter;
import info.ephyra.answerselection.filters.FactoidsFromPredicatesFilter;
import info.ephyra.answerselection.filters.PredicateExtractionFilter;
import info.ephyra.answerselection.filters.QuestionKeywordsFilter;
import info.ephyra.answerselection.filters.ScoreCombinationFilter;
import info.ephyra.answerselection.filters.ScoreNormalizationFilter;
import info.ephyra.answerselection.filters.ScoreSorterFilter;
import info.ephyra.answerselection.filters.StopwordFilter;
import info.ephyra.answerselection.filters.TruncationFilter;
import info.ephyra.answerselection.filters.WebDocumentFetcherFilter;
// import info.ephyra.io.Logger;
import info.ephyra.io.MsgPrinter;
import info.ephyra.nlp.LingPipe;
import info.ephyra.nlp.NETagger;
import info.ephyra.nlp.OpenNLP;
import info.ephyra.nlp.SnowballStemmer;
import info.ephyra.nlp.StanfordNeTagger;
import info.ephyra.nlp.StanfordParser;
import info.ephyra.nlp.indices.FunctionWords;
import info.ephyra.nlp.indices.IrregularVerbs;
import info.ephyra.nlp.indices.Prepositions;
import info.ephyra.nlp.indices.WordFrequencies;
import info.ephyra.nlp.semantics.ontologies.Ontology;
import info.ephyra.nlp.semantics.ontologies.WordNet;
import info.ephyra.querygeneration.Query;
import info.ephyra.querygeneration.QueryGeneration;
import info.ephyra.querygeneration.generators.BagOfTermsG;
import info.ephyra.querygeneration.generators.BagOfWordsG;
import info.ephyra.querygeneration.generators.PredicateG;
import info.ephyra.querygeneration.generators.QuestionInterpretationG;
import info.ephyra.querygeneration.generators.QuestionReformulationG;
import info.ephyra.questionanalysis.AnalyzedQuestion;
import info.ephyra.questionanalysis.QuestionAnalysis;
import info.ephyra.questionanalysis.QuestionInterpreter;
import info.ephyra.questionanalysis.QuestionNormalizer;
import info.ephyra.search.Result;
import info.ephyra.search.Search;
import info.ephyra.search.searchers.BingKM;
import info.ephyra.search.searchers.IndriKM;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import edu.umich.clarity.service.util.TClient;
import edu.umich.clarity.service.util.TServers;
import edu.umich.clarity.service.util.ServiceTypes;
import edu.umich.clarity.thrift.IPAService;
import edu.umich.clarity.thrift.QuerySpec;
import edu.umich.clarity.thrift.RegMessage;
import edu.umich.clarity.thrift.RegReply;
import edu.umich.clarity.thrift.SchedulerService;
import edu.umich.clarity.thrift.THostPort;
import edu.umich.clarity.thrift.LatencyStat;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * <code>OpenEphyra</code> is an open framework for question answering (QA).
 * 
 * @author Nico Schlaefer
 * @version 2008-03-23
 */
public class OpenEphyraServiceMulti implements IPAService.Iface {

	private static final String SERVICE_NAME = ServiceTypes.QA_SERVICE;
	private static final String SERVICE_INPUT_TYPE = ServiceTypes.SERVICE_INPUT_TEXT;
	private static final String SERVICE_IP = "clarity28.eecs.umich.edu";
	private static final int SERVICE_PORT = 9094;
	private static final String SCHEDULER_IP = "141.212.107.226";
	private static final int SCHEDULER_PORT = 8888;
	private static final Logger LOG = Logger.getLogger(OpenEphyraServiceMulti.class);

	// public static OpenEphyraService qaService;

	private static SchedulerService.Client scheduler_client;
	
	private BlockingQueue<OpenEphyra> qaInstanceQueue = new LinkedBlockingQueue<OpenEphyra>();

	private static final int QA_INSTANCE_NUM = 1;

	// private static final ExecutorService executor = Executors.newFixedThreadPool(WORKER_THREADS);
	public void initialize() {
		LOG.info("initialize " + QA_INSTANCE_NUM + " openEphyra instances pool in advance");
		for(int i = 0; i < QA_INSTANCE_NUM; i++) {
			OpenEphyra qaInstance = new OpenEphyra();
			qaInstanceQueue.offer(qaInstance);
			LOG.info("finished initializing the " + (i + 1) + " openEphyra instance into the pool");
		}
		try {
			scheduler_client = TClient.creatSchedulerClient(SCHEDULER_IP,
					SCHEDULER_PORT);
		} catch (IOException ex) {
			LOG.error("Error creating thrift scheduler client"
					+ ex.getMessage());
		}
		try {
			THostPort hostPort = new THostPort(SERVICE_IP, SERVICE_PORT);
			RegMessage regMessage = new RegMessage(SERVICE_NAME, hostPort);
			LOG.info("registering to command center runnig at " + SCHEDULER_IP
					+ ":" + SCHEDULER_PORT);
			scheduler_client.registerBackend(regMessage);
		} catch (TException ex) {
			LOG.error("Error registering backend service " + ex.getMessage());
		}
	}

	@Override
	public ByteBuffer submitQuery(QuerySpec query) throws TException {
		ByteBuffer result = null;
		// timestamp the query when it is enqueued (start)
		LOG.info("receiving text query...");
		
		long start_time = System.currentTimeMillis();
		byte[] input = query.getInputset().get(ServiceTypes.SERVICE_INPUT_TEXT).getInput();	
		OpenEphyra qaInstance = null;
		try {
			qaInstance = qaInstanceQueue.take();
		} catch (InterruptedException e) {
            		e.printStackTrace();
        	}	
		LOG.info("taking one OpenEphyra instance from the pool with " + qaInstanceQueue.size() + " more left");
		String answer = qaInstance.commandLine(new String(input).trim());
		result = ByteBuffer.wrap(answer.getBytes());
		long end_time = System.currentTimeMillis();

        	LatencyStat latencyStat = new LatencyStat();
        	latencyStat.setHostport(new THostPort(SERVICE_IP, SERVICE_PORT));
        	latencyStat.setLatency(end_time - start_time);
        	LOG.info("the latency for " + ServiceTypes.QA_SERVICE + " service is " + (end_time - start_time) + " ms");
        	LOG.info("update the latency statistics in command center...");
        	scheduler_client.updateLatencyStat(ServiceTypes.QA_SERVICE, latencyStat);
		// return the QA instance back to the pool
		qaInstanceQueue.offer(qaInstance);
		return result;
	}

	/**
	 * Entry point of Ephyra. Initializes the engine and starts the command line
	 * interface.
	 * 
	 * @param args command line arguments are ignored
	 */
	public static void main(String[] args) throws IOException, TException {
		// enable output of status and error messages
                MsgPrinter.enableStatusMsgs(true);
                MsgPrinter.enableErrorMsgs(true);

		// OpenEphyraService qaService = new OpenEphyraService();
		OpenEphyraServiceMulti qaService = new OpenEphyraServiceMulti();
		IPAService.Processor<IPAService.Iface> processor = new IPAService.Processor<IPAService.Iface>(
				qaService);
		TServers.launchThreadedThriftServer(SERVICE_PORT, processor);
		LOG.info("starting " + SERVICE_NAME + " service at " + SCHEDULER_IP
				+ ":" + SCHEDULER_PORT);
		qaService.initialize();
		// MsgPrinter.printStatusMsg("QA service is ready...");
		LOG.info("QA service is ready...");
		// set log file and enable logging
		// Logger.setLogfile("log/OpenEphyra");
		// Logger.enableLogging(true);
		
		// initialize Ephyra and start command line interface
		// (new OpenEphyra()).commandLine(args[0].trim());
	}
	/*
	private class QATaskCallable implements Callable<String> {
		private byte[] input;
		private OpenEphyra qaInstance = new OpenEphyra();
		public QATaskCallable(byte[] input) {
			this.input = input;
		}
		@Override
		public String call() throws Exception {
			
		}
	  }
	}
	*/
	private class OpenEphyra {	
	/** Factoid question type. */
        protected static final String FACTOID = "FACTOID";
        /** List question type. */
        protected static final String LIST = "LIST";

        /** Maximum number of factoid answers. */
        protected static final int FACTOID_MAX_ANSWERS = 1;
        /** Absolute threshold for factoid answer scores. */
        protected static final float FACTOID_ABS_THRESH = 0;
        /** Relative threshold for list answer scores (fraction of top score). */
        protected static final float LIST_REL_THRESH = 0.1f;

        /** Serialized classifier for score normalization. */
        public static final String NORMALIZER =
                "res/scorenormalization/classifiers/" +
                "AdaBoost70_" +
                "Score+Extractors_" +
                "TREC10+TREC11+TREC12+TREC13+TREC14+TREC15+TREC8+TREC9" +
                ".serialized";

        /** The directory of Ephyra, required when Ephyra is used as an API. */
        protected String dir;
	/**
	 * <p>Creates a new instance of Ephyra and initializes the system.</p>
	 * 
	 * <p>For use as a standalone system.</p>
	 */
	public OpenEphyra() {
		this("");
	}
	
	
	/**
	 * <p>Creates a new instance of Ephyra and initializes the system.</p>
	 * 
	 * <p>For use as an API.</p>
	 * 
	 * @param dir directory of Ephyra
	 */
	public OpenEphyra(String dir) {
		this.dir = dir;
		
		MsgPrinter.printInitializing();
		
		// create tokenizer
		MsgPrinter.printStatusMsg("Creating tokenizer...");
		if (!OpenNLP.createTokenizer(dir +
				"res/nlp/tokenizer/opennlp/EnglishTok.bin.gz"))
			MsgPrinter.printErrorMsg("Could not create tokenizer.");
//		LingPipe.createTokenizer();
		
		// create sentence detector
		MsgPrinter.printStatusMsg("Creating sentence detector...");
		if (!OpenNLP.createSentenceDetector(dir +
				"res/nlp/sentencedetector/opennlp/EnglishSD.bin.gz"))
			MsgPrinter.printErrorMsg("Could not create sentence detector.");
		LingPipe.createSentenceDetector();
		
		// create stemmer
		MsgPrinter.printStatusMsg("Creating stemmer...");
		SnowballStemmer.create();
		
		// create part of speech tagger
		MsgPrinter.printStatusMsg("Creating POS tagger...");
		if (!OpenNLP.createPosTagger(
				dir + "res/nlp/postagger/opennlp/tag.bin.gz",
				dir + "res/nlp/postagger/opennlp/tagdict"))
			MsgPrinter.printErrorMsg("Could not create OpenNLP POS tagger.");
//		if (!StanfordPosTagger.init(dir + "res/nlp/postagger/stanford/" +
//				"wsj3t0-18-bidirectional/train-wsj-0-18.holder"))
//			MsgPrinter.printErrorMsg("Could not create Stanford POS tagger.");
		
		// create chunker
		MsgPrinter.printStatusMsg("Creating chunker...");
		if (!OpenNLP.createChunker(dir +
				"res/nlp/phrasechunker/opennlp/EnglishChunk.bin.gz"))
			MsgPrinter.printErrorMsg("Could not create chunker.");
		
		// create syntactic parser
		MsgPrinter.printStatusMsg("Creating syntactic parser...");
//		if (!OpenNLP.createParser(dir + "res/nlp/syntacticparser/opennlp/"))
//			MsgPrinter.printErrorMsg("Could not create OpenNLP parser.");
		try {
			StanfordParser.initialize();
		} catch (Exception e) {
			MsgPrinter.printErrorMsg("Could not create Stanford parser.");
		}
		
		// create named entity taggers
		MsgPrinter.printStatusMsg("Creating NE taggers...");
		NETagger.loadListTaggers(dir + "res/nlp/netagger/lists/");
		NETagger.loadRegExTaggers(dir + "res/nlp/netagger/patterns.lst");
		MsgPrinter.printStatusMsg("  ...loading models");
//		if (!NETagger.loadNameFinders(dir + "res/nlp/netagger/opennlp/"))
//			MsgPrinter.printErrorMsg("Could not create OpenNLP NE tagger.");
		if (!StanfordNeTagger.isInitialized() && !StanfordNeTagger.init())
			MsgPrinter.printErrorMsg("Could not create Stanford NE tagger.");
		MsgPrinter.printStatusMsg("  ...done");
		
		// create linker
//		MsgPrinter.printStatusMsg("Creating linker...");
//		if (!OpenNLP.createLinker(dir + "res/nlp/corefresolver/opennlp/"))
//			MsgPrinter.printErrorMsg("Could not create linker.");
		
		// create WordNet dictionary
		MsgPrinter.printStatusMsg("Creating WordNet dictionary...");
		if (!WordNet.initialize(dir +
				"res/ontologies/wordnet/file_properties.xml"))
			MsgPrinter.printErrorMsg("Could not create WordNet dictionary.");
		
		// load function words (numbers are excluded)
		MsgPrinter.printStatusMsg("Loading function verbs...");
		if (!FunctionWords.loadIndex(dir +
				"res/indices/functionwords_nonumbers"))
			MsgPrinter.printErrorMsg("Could not load function words.");
		
		// load prepositions
		MsgPrinter.printStatusMsg("Loading prepositions...");
		if (!Prepositions.loadIndex(dir +
				"res/indices/prepositions"))
			MsgPrinter.printErrorMsg("Could not load prepositions.");
		
		// load irregular verbs
		MsgPrinter.printStatusMsg("Loading irregular verbs...");
		if (!IrregularVerbs.loadVerbs(dir + "res/indices/irregularverbs"))
			MsgPrinter.printErrorMsg("Could not load irregular verbs.");
		
		// load word frequencies
		MsgPrinter.printStatusMsg("Loading word frequencies...");
		if (!WordFrequencies.loadIndex(dir + "res/indices/wordfrequencies"))
			MsgPrinter.printErrorMsg("Could not load word frequencies.");
		
		// load query reformulators
		MsgPrinter.printStatusMsg("Loading query reformulators...");
		if (!QuestionReformulationG.loadReformulators(dir +
				"res/reformulations/"))
			MsgPrinter.printErrorMsg("Could not load query reformulators.");
		
		// load answer types
//		MsgPrinter.printStatusMsg("Loading answer types...");
//		if (!AnswerTypeTester.loadAnswerTypes(dir +
//				"res/answertypes/patterns/answertypepatterns"))
//			MsgPrinter.printErrorMsg("Could not load answer types.");
		
		// load question patterns
		MsgPrinter.printStatusMsg("Loading question patterns...");
		if (!QuestionInterpreter.loadPatterns(dir +
				"res/patternlearning/questionpatterns/"))
			MsgPrinter.printErrorMsg("Could not load question patterns.");
		
		// load answer patterns
		MsgPrinter.printStatusMsg("Loading answer patterns...");
		if (!AnswerPatternFilter.loadPatterns(dir +
				"res/patternlearning/answerpatterns/"))
			MsgPrinter.printErrorMsg("Could not load answer patterns.");
	}
	
	/**
	 * Reads a line from the command prompt.
	 * 
	 * @return user input
	 */
	protected String readLine() {
		try {
			return new java.io.BufferedReader(new
				java.io.InputStreamReader(System.in)).readLine();
		}
		catch(java.io.IOException e) {
			return new String("");
		}
	}
	
	/**
	 * Initializes the pipeline for factoid questions.
	 */
	protected void initFactoid() {
		// question analysis
		Ontology wordNet = new WordNet();
		// - dictionaries for term extraction
		QuestionAnalysis.clearDictionaries();
		QuestionAnalysis.addDictionary(wordNet);
		// - ontologies for term expansion
		QuestionAnalysis.clearOntologies();
		QuestionAnalysis.addOntology(wordNet);
		
		// query generation
		QueryGeneration.clearQueryGenerators();
		QueryGeneration.addQueryGenerator(new BagOfWordsG());
		QueryGeneration.addQueryGenerator(new BagOfTermsG());
		QueryGeneration.addQueryGenerator(new PredicateG());
		QueryGeneration.addQueryGenerator(new QuestionInterpretationG());
		QueryGeneration.addQueryGenerator(new QuestionReformulationG());
		
		// search
		// - knowledge miners for unstructured knowledge sources
		Search.clearKnowledgeMiners();
//		Search.addKnowledgeMiner(new BingKM());
//		Search.addKnowledgeMiner(new GoogleKM());
//		Search.addKnowledgeMiner(new YahooKM());
		for (String[] indriIndices : IndriKM.getIndriIndices())
			Search.addKnowledgeMiner(new IndriKM(indriIndices, false));
//		for (String[] indriServers : IndriKM.getIndriServers())
//			Search.addKnowledgeMiner(new IndriKM(indriServers, true));
		// - knowledge annotators for (semi-)structured knowledge sources
		Search.clearKnowledgeAnnotators();
		
		// answer extraction and selection
		// (the filters are applied in this order)
		AnswerSelection.clearFilters();
		// - answer extraction filters
		AnswerSelection.addFilter(new AnswerTypeFilter());
		AnswerSelection.addFilter(new AnswerPatternFilter());
		//AnswerSelection.addFilter(new WebDocumentFetcherFilter());
		AnswerSelection.addFilter(new PredicateExtractionFilter());
		AnswerSelection.addFilter(new FactoidsFromPredicatesFilter());
		AnswerSelection.addFilter(new TruncationFilter());
		// - answer selection filters
		AnswerSelection.addFilter(new StopwordFilter());
		AnswerSelection.addFilter(new QuestionKeywordsFilter());
		AnswerSelection.addFilter(new ScoreNormalizationFilter(NORMALIZER));
		AnswerSelection.addFilter(new ScoreCombinationFilter());
		AnswerSelection.addFilter(new FactoidSubsetFilter());
		AnswerSelection.addFilter(new DuplicateFilter());
		AnswerSelection.addFilter(new ScoreSorterFilter());
	}
	
	/**
	 * Runs the pipeline and returns an array of up to <code>maxAnswers</code>
	 * results that have a score of at least <code>absThresh</code>.
	 * 
	 * @param aq analyzed question
	 * @param maxAnswers maximum number of answers
	 * @param absThresh absolute threshold for scores
	 * @return array of results
	 */
	protected Result[] runPipeline(AnalyzedQuestion aq, int maxAnswers,
								  float absThresh) {
		// query generation
		MsgPrinter.printGeneratingQueries();
		Query[] queries = QueryGeneration.getQueries(aq);
		
		// search
		MsgPrinter.printSearching();
		Result[] results = Search.doSearch(queries);
		
		// answer selection
		MsgPrinter.printSelectingAnswers();
		results = AnswerSelection.getResults(results, maxAnswers, absThresh);
		
		return results;
	}
	
	/**
	 * Returns the directory of Ephyra.
	 * 
	 * @return directory
	 */
	public String getDir() {
		return dir;
	}
	
	/**
	 * <p>A command line interface for Ephyra.</p>
	 * 
	 * <p>Repeatedly queries the user for a question, asks the system the
	 * question and prints out and logs the results.</p>
	 * 
	 * <p>The command <code>exit</code> can be used to quit the program.</p>
	 */
	public String commandLine(String query_input) {
			String answers = "";
//		while (true) {
			// query user for question, quit if user types in "exit"
//			MsgPrinter.printQuestionPrompt();
//			String question = readLine().trim();

			String question = query_input.trim();

			if (question.equalsIgnoreCase("exit")) System.exit(0);
			
			// determine question type and extract question string
			String type;
			if (question.matches("(?i)" + FACTOID + ":.*+")) {
				// factoid question
				type = FACTOID;
				question = question.split(":", 2)[1].trim();
			} else if (question.matches("(?i)" + LIST + ":.*+")) {
				// list question
				type = LIST;
				question = question.split(":", 2)[1].trim();
			} else {
				// question type unspecified
				type = FACTOID;  // default type
			}
			
			// ask question
			Result[] results = new Result[0];
			if (type.equals(FACTOID)) {
				// Logger.logFactoidStart(question);
				results = askFactoid(question, FACTOID_MAX_ANSWERS,
						FACTOID_ABS_THRESH);
				// Logger.logResults(results);
				// Logger.logFactoidEnd();
			} else if (type.equals(LIST)) {
				// Logger.logListStart(question);
				results = askList(question, LIST_REL_THRESH);
				// Logger.logResults(results);
				// Logger.logListEnd();
			}
			
			// print answers
			// MsgPrinter.printAnswers(results);
			for (int i = 0; i < results.length; i++) {
                        	answers += "[" + (i + 1) + "]\t" +
                                                           results[i].getAnswer() + "\tScore: " + results[i].getScore();
                        	if (results[i].getDocID() != null)
                                	answers += "\tDocument: " + results[i].getDocID();
                		}
				answers += "\n";
			return answers;
		//}
	}
	
	/**
	 * Asks Ephyra a factoid question and returns up to <code>maxAnswers</code>
	 * results that have a score of at least <code>absThresh</code>.
	 * 
	 * @param question factoid question
	 * @param maxAnswers maximum number of answers
	 * @param absThresh absolute threshold for scores
	 * @return array of results
	 */
	public Result[] askFactoid(String question, int maxAnswers,
							   float absThresh) {
		// initialize pipeline
		initFactoid();
		
		// analyze question
		MsgPrinter.printAnalyzingQuestion();
		AnalyzedQuestion aq = QuestionAnalysis.analyze(question);
		
		// get answers
		Result[] results = runPipeline(aq, maxAnswers, absThresh);
		
		return results;
	}
	
	/**
	 * Asks Ephyra a factoid question and returns a single result or
	 * <code>null</code> if no answer could be found.
	 * 
	 * @param question factoid question
	 * @return single result or <code>null</code>
	 */
	public Result askFactoid(String question) {
		Result[] results = askFactoid(question, 1, 0);
		
		return (results.length > 0) ? results[0] : null;
	}
	
	/**
	 * Asks Ephyra a list question and returns results that have a score of at
	 * least <code>relThresh * top score</code>.
	 * 
	 * @param question list question
	 * @param relThresh relative threshold for scores
	 * @return array of results
	 */
	public Result[] askList(String question, float relThresh) {
		question = QuestionNormalizer.transformList(question);
		
		Result[] results = askFactoid(question, Integer.MAX_VALUE, 0);
		
		// get results with a score of at least relThresh * top score
		ArrayList<Result> confident = new ArrayList<Result>();
		if (results.length > 0) {
			float topScore = results[0].getScore();
			
			for (Result result : results)
				if (result.getScore() >= relThresh * topScore)
					confident.add(result);
		}
		
		return confident.toArray(new Result[confident.size()]);
	}
    }
}
