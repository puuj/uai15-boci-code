package edu.umd.cs.onlineinfer.jester

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.google.common.collect.Iterables

import edu.umd.cs.onlineinfer.jester.AdjCosineSimilarity;
import edu.umd.cs.onlineinfer.jester.ProjectionAverage;
import edu.umd.cs.onlineinfer.util.DataOutputter;
import edu.umd.cs.onlineinfer.util.ExperimentConfigGenerator;
import edu.umd.cs.onlineinfer.util.WeightLearner;
import edu.umd.cs.psl.application.inference.MPEInference
import edu.umd.cs.psl.application.inference.OnlineMPEInference
import edu.umd.cs.psl.application.learning.weight.maxmargin.MaxMargin.LossBalancingType
import edu.umd.cs.psl.application.learning.weight.maxmargin.MaxMargin.NormScalingType
import edu.umd.cs.psl.config.*
import edu.umd.cs.psl.database.DataStore
import edu.umd.cs.psl.database.Database
import edu.umd.cs.psl.database.DatabasePopulator
import edu.umd.cs.psl.database.Partition
import edu.umd.cs.psl.database.ResultList
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore
import edu.umd.cs.psl.database.rdbms.DatabaseComparer
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type
import edu.umd.cs.psl.evaluation.result.*;
import edu.umd.cs.psl.evaluation.statistics.ContinuousPredictionComparator
import edu.umd.cs.psl.groovy.*
import edu.umd.cs.psl.model.argument.ArgumentType
import edu.umd.cs.psl.model.argument.GroundTerm
import edu.umd.cs.psl.model.argument.UniqueID
import edu.umd.cs.psl.model.argument.Variable
import edu.umd.cs.psl.model.atom.GroundAtom
import edu.umd.cs.psl.model.atom.QueryAtom
import edu.umd.cs.psl.model.atom.RandomVariableAtom
import edu.umd.cs.psl.model.kernel.CompatibilityKernel
import edu.umd.cs.psl.model.predicate.StandardPredicate
import edu.umd.cs.psl.model.parameters.Weight
import edu.umd.cs.psl.reasoner.admm.ADMMStateActivator
import edu.umd.cs.psl.ui.loading.*
import edu.umd.cs.psl.util.database.Queries


class JesterApproximate {

	Logger log = LoggerFactory.getLogger(this.class);
	

	
	class JesterConfig {
		public String modelType;
		public boolean squared;
		public int num_jokes;
		public int num_users;
		public int num_sequences;
		public String scoring_method;
		public String dataPath;
		public String outPath;
		public String ratingsPath;
		public String dbPath;
		public double simThresh;
		public int[] epochs;
		public Partition[] ratingPartitions;
		public Map<String,Partition> partitionMap;
	}

	def makeConfig(args){
		def config = new JesterConfig();
		
		config.modelType = args[0];
		config.squared = (!config.modelType.equals("linear") ? true : false);
		
		/* Experiment dimensions */
		config.num_jokes = 100;
		config.num_users = Integer.parseInt(args[1]).value;
		config.num_sequences = Integer.parseInt(args[2]).value;
		config.scoring_method = args[3];

		//def epochs = [25, 35, 45, 55, 65, 75];
		config.epochs = [25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75];

		config.dataPath = "../data/";
		config.outPath = "./out-" + config.num_users + "-" + config.scoring_method+  "/";
		config.ratingsPath = config.dataPath + "ratings-" + config.num_users + "/";

		ConfigManager cm = ConfigManager.getManager();
		ConfigBundle cb = cm.getBundle("jester");

		def defPath = System.getProperty("java.io.tmpdir") + "/psl-jester";
		config.dbPath = cb.getString("dbpath", defPath);

		config.simThresh = cb.getDouble("simThresh", 0.5);
		config.partitionMap = new HashMap<String,Partition>();
		return config;
	}

	def defineModel(data, m, config){
	/* PREDICATES */
	m.add predicate: "user", types: [ArgumentType.UniqueID];
	m.add predicate: "joke", types: [ArgumentType.UniqueID];
	m.add predicate: "rating", types: [ArgumentType.UniqueID,ArgumentType.UniqueID];
	m.add predicate: "ratingObs", types: [ArgumentType.UniqueID,ArgumentType.UniqueID];
	m.add predicate: "ratingPrior", types: [ArgumentType.UniqueID];
	m.add predicate: "avgUserRatingObs", types: [ArgumentType.UniqueID];
	m.add predicate: "avgJokeRatingObs", types: [ArgumentType.UniqueID];
	m.add predicate: "simObsRating", types: [ArgumentType.UniqueID,ArgumentType.UniqueID];

	/* RULES */
	boolean sq = config.squared;
	// If J1,J2 have similar observed ratings, then U will rate them similarly
	m.add rule: ( simObsRating(J1,J2) & rating(U,J1) ) >> rating(U,J2), weight: 1.0, squared: sq;

	// Ratings should concentrate around observed user/joke averages
	m.add rule: ( user(U) & joke(J) & avgUserRatingObs(U) ) >> rating(U,J), weight: 1.0, squared: sq;
	m.add rule: ( user(U) & joke(J) & avgJokeRatingObs(J) ) >> rating(U,J), weight: 1.0, squared: sq;
	m.add rule: ( user(U) & joke(J) & rating(U,J) ) >> avgUserRatingObs(U), weight: 1.0, squared: sq;
	m.add rule: ( user(U) & joke(J) & rating(U,J) ) >> avgJokeRatingObs(J), weight: 1.0, squared: sq;

	// Two-sided prior
	UniqueID constant = data.getUniqueID(0)
			m.add rule: ( user(U) & joke(J) & ratingPrior(constant) ) >> rating(U,J), weight: 1.0, squared: sq;
			m.add rule: ( rating(U,J) ) >> ratingPrior(constant), weight: 1.0, squared: sq;

	return m;

	}
	
	def populateInitWeights(data,m){
		/* get all default weights */
		Map<CompatibilityKernel,Weight> initWeights = new HashMap<CompatibilityKernel, Weight>()
				for (CompatibilityKernel k : Iterables.filter(m.getKernels(), CompatibilityKernel.class))
					initWeights.put(k, k.getWeight());
		return initWeights;
	}
	
	def loadStaticData(data,m,config){

		int cur_partition = 0;
		def inserter;

		/* Load the static data into static (i.e., for the life of the program) partitions. */

		// jokes
		Partition joke_part = data.getPartition("jokes");
		inserter = data.getInserter(joke, joke_part);
		InserterUtils.loadDelimitedData(inserter, config.dataPath + "/jokes.txt");
		// users
		Partition user_part_tr = data.getPartition("users_tr");
		inserter = data.getInserter(user, user_part_tr);
		InserterUtils.loadDelimitedData(inserter, config.dataPath + "/users-tr-" + config.num_users + ".txt");
		Partition user_part_te = data.getPartition("users_te");
		inserter = data.getInserter(user, user_part_te);
		InserterUtils.loadDelimitedData(inserter, config.dataPath + "/users-te-" + config.num_users + ".txt");
		
		config.partitionMap.put("jokes",joke_part);
		config.partitionMap.put("users_tr",user_part_tr);
		config.partitionMap.put("users_te",user_part_te);
	}
	
	def loadTrainingData(data,m,config,sequence){
		
		def inserter;
		
		/* Partitions */
		Partition read_tr = data.getPartition("tr_read");
		Partition write_tr = data.getPartition("tr_write");
		Partition labels_tr = data.getPartition("tr_labels");

		/* Create an atom for the prior (we'll overwrite later). */
		UniqueID constant = data.getUniqueID(0);
		data.getInserter(ratingPrior, read_tr).insertValue(0.5, constant);

		/* Load training data. */
		inserter = data.getInserter(rating, read_tr);
		InserterUtils.loadDelimitedDataTruth(inserter, config.ratingsPath + "jester-" + config.num_users + "-tr-obs-" + sequence + ".txt");
		inserter = data.getInserter(ratingObs, read_tr);
		InserterUtils.loadDelimitedDataTruth(inserter, config.ratingsPath + "jester-" + config.num_users + "-tr-obs-" + sequence + ".txt");
		inserter = data.getInserter(rating, labels_tr);
		InserterUtils.loadDelimitedDataTruth(inserter, config.ratingsPath + "jester-" + config.num_users + "-tr-uno-" + sequence + ".txt");
	
		config.partitionMap.put("tr_read",read_tr);
		config.partitionMap.put("tr_write",write_tr);
		config.partitionMap.put("tr_labels",labels_tr);
		
	}
	
	def loadTestingData(data, m, config, sequence){
		/* Load testing data. */
		def inserter;
		config.ratingPartitions = new Partition[config.epochs.size()];
		for (int e = 0; e < config.epochs.size(); e++) {
			config.ratingPartitions[e] = data.getPartition("ratings_test_"+Integer.toString(e));
			inserter = data.getInserter(rating, config.ratingPartitions[e]);
			InserterUtils.loadDelimitedDataTruth(inserter, config.ratingsPath + "jester-" + config.num_users + "-te-obs-" + sequence + "-" + e + ".txt");
			inserter = data.getInserter(ratingObs, config.ratingPartitions[e]);
			InserterUtils.loadDelimitedDataTruth(inserter, config.ratingsPath + "jester-" + config.num_users + "-te-obs-" + sequence + "-" + e + ".txt");
		}
		
	}
	
	def populateTrainingDatabase(data, m, config){
		/* We want to populate the database with all groundings 'rating' and 'ratingObs'
		 * To do so, we will query for all users and jokes in train/test, then use the
		 * database populator to compute the cross-product. 
		 */

		DatabasePopulator dbPop;
		Variable User = new Variable("User");
		Variable Joke = new Variable("Joke");
		Set<GroundTerm> users = new HashSet<GroundTerm>();
		Set<GroundTerm> jokes = new HashSet<GroundTerm>();
		Map<Variable, Set<GroundTerm>> subs = new HashMap<Variable, Set<GroundTerm>>();
		subs.put(User, users);
		subs.put(Joke, jokes);
		ResultList results;
		def toClose;
		ProjectionAverage userAverager = new ProjectionAverage(ratingObs, 1);
		ProjectionAverage jokeAverager = new ProjectionAverage(ratingObs, 0);
		AdjCosineSimilarity userCosSim = new AdjCosineSimilarity(ratingObs, 1, avgJokeRatingObs, config.simThresh);
		AdjCosineSimilarity jokeCosSim = new AdjCosineSimilarity(ratingObs, 0, avgUserRatingObs, config.simThresh);

		/* Precompute average ratings. */
		log.info("Computing averages ...")
		Database trainDB = data.getDatabase(config.partitionMap.get("tr_read"), config.partitionMap.get("jokes"), config.partitionMap.get("users_tr"));
		results = trainDB.executeQuery(Queries.getQueryForAllAtoms(user));
		for (int i = 0; i < results.size(); i++) {
			GroundTerm u = results.get(i)[0];
			users.add(u);
			double avg = userAverager.getValue(trainDB, u);
			RandomVariableAtom a = (RandomVariableAtom) trainDB.getAtom(avgUserRatingObs, u);
			a.setValue(avg);
			trainDB.commit(a);
		}
		results = trainDB.executeQuery(Queries.getQueryForAllAtoms(joke));
		for (int i = 0; i < results.size(); i++) {
			GroundTerm j = results.get(i)[0];
			jokes.add(j);
			double avg = jokeAverager.getValue(trainDB, j);
			RandomVariableAtom a = (RandomVariableAtom) trainDB.getAtom(avgJokeRatingObs, j);
			a.setValue(avg);
			trainDB.commit(a);
		}

		/* Compute the prior as average over all observed ratings.
		 * (This is not the most efficient way of doing this. We should be able to 
		 * compute the average overall rating when we compute user/item averages.)
		 */
		UniqueID constant = data.getUniqueID(0);
		double avgAllRatingObs = 0.0;
		Set<GroundAtom> allRatingObs = Queries.getAllAtoms(trainDB, ratingObs);
		for (GroundAtom a : allRatingObs) {
			avgAllRatingObs += a.getValue();
		}
		avgAllRatingObs /= allRatingObs.size();
		log.info("  Average rating (train): {}", avgAllRatingObs);
		RandomVariableAtom priorAtom = (RandomVariableAtom) trainDB.getAtom(ratingPrior, constant);
		priorAtom.setValue(avgAllRatingObs);
		

		/* Precompute the similarities. */
		log.info("Computing training similarities ...")
		int nnzSim = 0;
		double avgsim = 0.0;
		List<GroundTerm> jokeList = new ArrayList(jokes);
		for (int i = 0; i < jokeList.size(); i++) {
			GroundTerm j1 = jokeList.get(i);
			for (int j = i+1; j < jokeList.size(); j++) {
				GroundTerm j2 = jokeList.get(j);
				double s = jokeCosSim.getValue(trainDB, j1, j2);
				if (s > 0.0) {
					/* upper half */
					RandomVariableAtom a = (RandomVariableAtom) trainDB.getAtom(simObsRating, j1, j2);
					a.setValue(s);
					trainDB.commit(a);
					/* lower half */
					a = (RandomVariableAtom) trainDB.getAtom(simObsRating, j2, j1);
					a.setValue(s);
					trainDB.commit(a);
					/* update stats */
					++nnzSim;
					avgsim += s;
				}
			}
		}
		log.info("  Number nonzero sim (train): {}", nnzSim);
		log.info("  Average joke rating sim (train): {}", avgsim / nnzSim);
		trainDB.close();

		/* Populate the user-joke cross product in training data. */
		log.info("Populating training database ...");
		toClose = [user,joke,ratingObs,ratingPrior,avgUserRatingObs,avgJokeRatingObs,simObsRating] as Set;
		trainDB = data.getDatabase(config.partitionMap.get("tr_write"), toClose, config.partitionMap.get("tr_read"), config.partitionMap.get("jokes"), config.partitionMap.get("users_tr"));
		dbPop = new DatabasePopulator(trainDB);
		dbPop.populate(new QueryAtom(rating, User, Joke), subs);
		
		return trainDB;
	}
		
	def learnModelWeights(data,m,config,initWeights,trainDB,cb){
		Database labelsDB = data.getDatabase(config.partitionMap.get("tr_labels"), [rating] as Set);
		def configName = cb.getString("name", "");
		def method = cb.getString("learningmethod", "");
		WeightLearner.learn(method, m, trainDB, labelsDB, initWeights, cb, log)
		log.info("Learned model {}: \n {}", configName, m.toString())
		trainDB.close()
		labelsDB.close();
		return m;
	}
	
def cleanupTrainingData(data,m,config){
	data.deletePartition(data.getPartition("tr_read"));
	data.deletePartition(data.getPartition("tr_write"));
	data.deletePartition(data.getPartition("tr_labels"));		
}

	def populateTestingDatabaseObservations(data,m,config,sequence,epoch){
		/* Create new test partitions. (Wasteful, but I don't see a way around it.) */
		Partition read_te = data.getPartition("te_read_"+Integer.toString(epoch));

		/* Combine observations from current epoch. */
		def read_partition_list = new ArrayList<Partition>();
		read_partition_list.add(config.partitionMap.get("jokes"));
		read_partition_list.add(config.partitionMap.get("users_te"));
		for (int e = 0; e <= epoch; e++) {
			read_partition_list.add(config.ratingPartitions[e]);
		}

		Set<GroundTerm> users = new HashSet<GroundTerm>();
		Set<GroundTerm> jokes = new HashSet<GroundTerm>();

		/* Create an atom for the prior (we'll overwrite later). */
		UniqueID constant = data.getUniqueID(0);
		data.getInserter(ratingPrior, read_te).insertValue(0.5, constant);
		
		ProjectionAverage userAverager = new ProjectionAverage(ratingObs, 1);
		ProjectionAverage jokeAverager = new ProjectionAverage(ratingObs, 0);
		AdjCosineSimilarity userCosSim = new AdjCosineSimilarity(ratingObs, 1, avgJokeRatingObs, config.simThresh);
		AdjCosineSimilarity jokeCosSim = new AdjCosineSimilarity(ratingObs, 0, avgUserRatingObs, config.simThresh);


		/* Get the test set users/jokes and precompute averages. */
		log.info("Computing averages ...")
		Database testDB = data.getDatabase(read_te, read_partition_list.toArray(new Partition[0]));
		def results = testDB.executeQuery(Queries.getQueryForAllAtoms(user));
		for (int i = 0; i < results.size(); i++) {
			GroundTerm u = results.get(i)[0];
			users.add(u);
			double avg = userAverager.getValue(testDB, u);
			RandomVariableAtom a = (RandomVariableAtom) testDB.getAtom(avgUserRatingObs, u);
			a.setValue(avg);
			testDB.commit(a);
		}
		results = testDB.executeQuery(Queries.getQueryForAllAtoms(joke));
		for (int i = 0; i < results.size(); i++) {
			GroundTerm j = results.get(i)[0];
			jokes.add(j);
			double avg = jokeAverager.getValue(testDB, j);
			RandomVariableAtom a = (RandomVariableAtom) testDB.getAtom(avgJokeRatingObs, j);
			a.setValue(avg);
			testDB.commit(a);
		}

		/* Compute the prior as average over all observed ratings. */
		def avgAllRatingObs = 0.0;
		def allRatingObs = Queries.getAllAtoms(testDB, ratingObs);
		for (GroundAtom a : allRatingObs) {
			avgAllRatingObs += a.getValue();
		}
		avgAllRatingObs /= allRatingObs.size();
		log.info("  Average rating (test): {}", avgAllRatingObs);
		def priorAtom = (RandomVariableAtom) testDB.getAtom(ratingPrior, constant);
		priorAtom.setValue(avgAllRatingObs);

		/* Precompute the similarities. */
		log.info("Computing testing similarities ...");
		int nnzSim = 0;
		double avgsim = 0.0;
		List<GroundTerm> jokeList = new ArrayList(jokes);
		for (int i = 0; i < jokeList.size(); i++) {
			GroundTerm j1 = jokeList.get(i);
			for (int j = i+1; j < jokeList.size(); j++) {
				GroundTerm j2 = jokeList.get(j);
				double s = jokeCosSim.getValue(testDB, j1, j2);
				if (s > 0.0) {
					/* upper half */
					RandomVariableAtom a = (RandomVariableAtom) testDB.getAtom(simObsRating, j1, j2);
					a.setValue(s);
					testDB.commit(a);
					/* lower half */
					a = (RandomVariableAtom) testDB.getAtom(simObsRating, j2, j1);
					a.setValue(s);
					testDB.commit(a);
					/* update stats */
					++nnzSim;
					avgsim += s;
				}
			}
		}
		log.info("  Number nonzero sim (test): {}", nnzSim);
		log.info("  Average joke rating sim (test): {}", avgsim / nnzSim);
		testDB.close();

		/* Push read_te to top of read_partition_list. */
		read_partition_list.push(read_te);
		
		return [read_partition_list, users, jokes];
	}
	def populateTestingDatabaseTargets(data,m,config,sequence,epoch, read_partition_list, users, jokes){
		/* Populate testing database. */
		log.info("Populating testing database ...");
		Partition write_te = data.getPartition("te_write_"+Integer.toString(epoch));

		DatabasePopulator dbPop;
		Variable User = new Variable("User");
		Variable Joke = new Variable("Joke");
		Map<Variable, Set<GroundTerm>> subs = new HashMap<Variable, Set<GroundTerm>>();
		subs.put(User, users);
		subs.put(Joke, jokes);

		def toClose = [user,joke,ratingObs,ratingPrior,avgUserRatingObs,avgJokeRatingObs,simObsRating] as Set;
		Database testDB = data.getDatabase(write_te, toClose, read_partition_list.toArray(new Partition[0]));
		dbPop = new DatabasePopulator(testDB);
		dbPop.populate(new QueryAtom(rating, User, Joke), subs);
		testDB.close();
	
	}
	
	def inferTestVars(data,m,config,sequence,epoch,cb,read_partition_list){
		/* Inference on test set */
		def toClose = [user,joke,ratingObs,ratingPrior,avgUserRatingObs,avgJokeRatingObs,simObsRating] as Set;
		Partition read_te = data.getPartition("te_read_"+Integer.toString(epoch));
		Partition write_te = data.getPartition("te_write_"+Integer.toString(epoch));
		
		Database predDB = data.getDatabase(write_te, toClose, read_partition_list.toArray(new Partition[0]));
		Set<GroundAtom> allAtoms = Queries.getAllAtoms(predDB, rating);
		for (RandomVariableAtom atom : Iterables.filter(allAtoms, RandomVariableAtom)) {
			atom.setValue(0.0);
		}
		OnlineMPEInference mpe = new OnlineMPEInference(m, predDB, cb, null, new HashSet<QueryAtom>())
		def result = mpe.mpeInference();
		log.info("Objective: {}", result.getTotalWeightedIncompatibility());
		def outFilename = config.outPath + "jester-" + config.num_users + "-preds-" + sprintf('%02d',sequence) + "-" + sprintf('%02d',epoch) + ".out";
		log.info("Writing to file: {}", outFilename);
		DataOutputter.outputPredicate(outFilename, predDB, rating, "\t", true, null);
		predDB.close();
		data.deletePartition(write_te);
		data.deletePartition(read_te);
		return result;
	}
	
	def inferTestVarsOnline(data,m,config,sequence,epoch,cb,read_partition_list,activator,atomsToConsider){
		/* Inference on test set */
		def toClose = [user,joke,ratingObs,ratingPrior,avgUserRatingObs,avgJokeRatingObs,simObsRating] as Set;
		Partition read_te = data.getPartition("te_read_"+Integer.toString(epoch));
		Partition write_te = data.getPartition("te_write_"+Integer.toString(epoch));
		
		Database predDB = data.getDatabase(write_te, toClose, read_partition_list.toArray(new Partition[0]));
		OnlineMPEInference mpe = new OnlineMPEInference(m, predDB, cb, activator, atomsToConsider)
		FullInferenceResult result = mpe.mpeInference()
		log.info("Objective: {}", result.getTotalWeightedIncompatibility());
		def outFilename = config.outPath + "jester-" + config.num_users + "-preds-" + sprintf('%02d',sequence) + "-" + sprintf('%02d',epoch) + ".out";
		log.info("Writing to file: {}", outFilename);
		DataOutputter.outputPredicate(outFilename, predDB, rating, "\t", true, null);
		predDB.close();
		data.deletePartition(write_te);
		data.deletePartition(read_te);
		return result;
	}
	
	def cleanupTestingData(data,m,config){
		for (int e = 0; e < config.epochs.size(); e++) {
			data.deletePartition(config.ratingPartitions[e]);
		}
	}

	def findUpdatedRatingsSimple(data,m,config,epoch){
		Database updateDB =  data.getDatabase(config.ratingPartitions[epoch]);
		Set<QueryAtom> atomsToConsider = new HashSet<QueryAtom>();
		for (GroundAtom a : Queries.getAllAtoms(updateDB, rating)){
			atomsToConsider.add(new QueryAtom(a.getPredicate(),a.getArguments()));
		}
		updateDB.close();
		return atomsToConsider;
	}
	
	def findUpdatedRatings(data,m,config,epoch){
		/* Combine observations from current epoch. */
		def read_partition_list = new ArrayList<Partition>();
		for (int e = 0; e <= epoch-1; e++) {
			read_partition_list.add(config.ratingPartitions[e]);
		}
		Database prevDB = data.getDatabase(data.getPartition("dummy_prev_"+epoch),read_partition_list.toArray(new Partition[0]));
		read_partition_list.push(config.ratingPartitions[epoch]);
		Database currDB = data.getDatabase(data.getPartition("dummy_curr_"+epoch),read_partition_list.toArray(new Partition[0]));
		
		DatabaseComparer dbc = new DatabaseComparer(data,prevDB,currDB);
		Set<QueryAtom> atomsToConsider = new HashSet<QueryAtom>();
		for( StandardPredicate p : new ArrayList([rating]) ){ 
			atomsToConsider.addAll(dbc.queryUpdatedAtoms(p));
			atomsToConsider.addAll(dbc.queryAddedAtoms(p));
			atomsToConsider.addAll(dbc.queryRemovedAtoms(p));
		}
		prevDB.close();
		currDB.close();
		return atomsToConsider;
}
		
	
	static void main(args){
		def jm = new JesterApproximate();
		
		/*** CONFIGURATION PARAMETERS ***/
		def config = jm.makeConfig(args)
		
		ConfigManager cm = ConfigManager.getManager();
		ConfigBundle cb = cm.getBundle("jester");


		/****** Datastore *******/
		DataStore data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, config.dbPath, true), cb);

		ExperimentConfigGenerator configGenerator = new ExperimentConfigGenerator("jester");

		/*
		 * SET MODEL TYPES
		 *
		 * Options:
		 * "quad" HL-MRF-Q
		 * "linear" HL-MRF-L
		 * "bool" MRF
		 */
		configGenerator.setModelTypes([config.modelType]);

		/*
		 * SET LEARNING ALGORITHMS
		 *
		 * Options:
		 * "MLE" (MaxLikelihoodMPE)
		 * "MPLE" (MaxPseudoLikelihood)
		 * "MM" (MaxMargin)
		 */
		def methods = ["MLE"];
		configGenerator.setLearningMethods(methods);

		/* MLE/MPLE options */
		configGenerator.setVotedPerceptronStepCounts([1000]);
		configGenerator.setVotedPerceptronStepSizes([(double) 1.0]);

		List<ConfigBundle> configs = configGenerator.getConfigs();


		/*** MODEL DEFINITION ***/

		jm.log.info("Initializing model ...");

		PSLModel m = new PSLModel(jm, data);
		m = jm.defineModel(data, m, config);
		def initWeights = jm.populateInitWeights(data,m);
		//log.info("Model: {}", m)

		/*** LOAD DATA ***/
		jm.log.info("Loading data ...");
		jm.loadStaticData(data,m,config);


		/** LOOP OVER SEQUENCES **/
		for (int sequence = 0; sequence < config.num_sequences; sequence++) {
			/** LOAD DATA FILES **/
			jm.loadTrainingData(data,m,config,sequence);
			jm.loadTestingData(data,m,config,sequence);

			/** POPULATE TRAINING DATA ***/
			def trainDB = jm.populateTrainingDatabase(data,m,config);

			/** TRAINING **/
			jm.log.info("Starting training ...");
			ConfigBundle cbSeq = configs.get(0);
			cbSeq.setProperty("activator.scoring_method",config.scoring_method);

			m = jm.learnModelWeights(data,m,config,initWeights,trainDB,cbSeq);
			jm.cleanupTrainingData(data,m,config);

			/** ONLINE INFERENCE ON TEST SET **/
			jm.log.info("Starting online inference ...");

			Database prevDB = null;
			def result;
			for (int epoch = 0; epoch < config.epochs.size(); epoch++) {
				jm.log.info("Starting epoch {}", epoch);
				def dbVars = jm.populateTestingDatabaseObservations(data,m,config,sequence,epoch);
				if(epoch == 0){
					jm.populateTestingDatabaseTargets(data,m,config,sequence,epoch,dbVars[0], dbVars[1], dbVars[2]);
					result = jm.inferTestVars(data,m,config,sequence,epoch,cbSeq,dbVars[0]);
				} else {
					def atomsToConsider = jm.findUpdatedRatingsSimple(data,m,config,epoch);
					def activator = new ADMMStateActivator(result.getADMMReasonerState(), cbSeq)
					result = jm.inferTestVarsOnline(data,m,config,sequence,epoch,cbSeq,dbVars[0],activator,atomsToConsider);
				}
			}
			jm.cleanupTestingData(data,m,config);

		}
		data.close();
	}
}