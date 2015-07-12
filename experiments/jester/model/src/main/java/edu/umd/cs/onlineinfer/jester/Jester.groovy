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
import edu.umd.cs.psl.application.learning.weight.maxmargin.MaxMargin.LossBalancingType
import edu.umd.cs.psl.application.learning.weight.maxmargin.MaxMargin.NormScalingType
import edu.umd.cs.psl.config.*
import edu.umd.cs.psl.database.DataStore
import edu.umd.cs.psl.database.Database
import edu.umd.cs.psl.database.DatabasePopulator
import edu.umd.cs.psl.database.Partition
import edu.umd.cs.psl.database.ResultList
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type
import edu.umd.cs.psl.evaluation.result.FullInferenceResult
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
import edu.umd.cs.psl.model.parameters.Weight
import edu.umd.cs.psl.ui.loading.*
import edu.umd.cs.psl.util.database.Queries


/*** CONFIGURATION PARAMETERS ***/

def modelType = args[0];
sq = (!modelType.equals("linear") ? true : false)

Logger log = LoggerFactory.getLogger(this.class)
ConfigManager cm = ConfigManager.getManager();
ConfigBundle cb = cm.getBundle("jester");

def defPath = System.getProperty("java.io.tmpdir") + "/psl-jester";
def dbpath = cb.getString("dbpath", defPath);
DataStore data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, dbpath, true), cb);

/* Experiment dimensions */
int num_jokes = 100;
int num_users = Integer.parseInt(args[1]).value;
int num_sequences = Integer.parseInt(args[2]).value;
//def epochs = [25, 35, 45, 55, 65, 75];
def epochs = [25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75];

def dataPath = "../data/";
def outPath = "./out-" + num_users + "/";
def ratingsPath = dataPath + "ratings-" + num_users + "/";

def simThresh = cb.getDouble("simThresh", 0.5);

ExperimentConfigGenerator configGenerator = new ExperimentConfigGenerator("jester");

/*
 * SET MODEL TYPES
 *
 * Options:
 * "quad" HL-MRF-Q
 * "linear" HL-MRF-L
 * "bool" MRF
 */
configGenerator.setModelTypes([modelType]);

/*
 * SET LEARNING ALGORITHMS
 *
 * Options:
 * "MLE" (MaxLikelihoodMPE)
 * "MPLE" (MaxPseudoLikelihood)
 * "MM" (MaxMargin)
 */
methods = ["MLE"];
configGenerator.setLearningMethods(methods);

/* MLE/MPLE options */
configGenerator.setVotedPerceptronStepCounts([1000]);
configGenerator.setVotedPerceptronStepSizes([(double) 1.0]);

List<ConfigBundle> configs = configGenerator.getConfigs();


/*** MODEL DEFINITION ***/

log.info("Initializing model ...");

PSLModel m = new PSLModel(this, data);

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

//log.info("Model: {}", m)

/* get all default weights */
Map<CompatibilityKernel,Weight> initWeights = new HashMap<CompatibilityKernel, Weight>()
for (CompatibilityKernel k : Iterables.filter(m.getKernels(), CompatibilityKernel.class))
	initWeights.put(k, k.getWeight());

/*** LOAD DATA ***/

log.info("Loading data ...");

int cur_partition = 0;
def inserter;

/* Load the static data into static (i.e., for the life of the program) partitions. */

// jokes
Partition joke_part = data.getPartition(Integer.toString(cur_partition++));
inserter = data.getInserter(joke, joke_part);
InserterUtils.loadDelimitedData(inserter, dataPath + "/jokes.txt");
// users
Partition user_part_tr = data.getPartition(Integer.toString(cur_partition++));
inserter = data.getInserter(user, user_part_tr);
InserterUtils.loadDelimitedData(inserter, dataPath + "/users-tr-" + num_users + ".txt");
Partition user_part_te = data.getPartition(Integer.toString(cur_partition++));
inserter = data.getInserter(user, user_part_te);
InserterUtils.loadDelimitedData(inserter, dataPath + "/users-te-" + num_users + ".txt");


/** LOOP OVER SEQUENCES **/

for (int sequence = 0; sequence < num_sequences; sequence++) {
	
	/** LOAD DATA FILES **/

	/* Partitions */
  Partition read_tr = data.getPartition(Integer.toString(cur_partition++));
  Partition write_tr = data.getPartition(Integer.toString(cur_partition++));
  Partition labels_tr = data.getPartition(Integer.toString(cur_partition++));
	Partition read_te;
	Partition write_te;

	/* Create an atom for the prior (we'll overwrite later). */
	data.getInserter(ratingPrior, read_tr).insertValue(0.5, constant);

	/* Load training data. */
	inserter = data.getInserter(rating, read_tr);
	InserterUtils.loadDelimitedDataTruth(inserter, ratingsPath + "jester-" + num_users + "-tr-obs-" + sequence + ".txt");
	inserter = data.getInserter(ratingObs, read_tr);
	InserterUtils.loadDelimitedDataTruth(inserter, ratingsPath + "jester-" + num_users + "-tr-obs-" + sequence + ".txt");
	inserter = data.getInserter(rating, labels_tr);
	InserterUtils.loadDelimitedDataTruth(inserter, ratingsPath + "jester-" + num_users + "-tr-uno-" + sequence + ".txt");
	
	/* Load testing data. */
	Partition[] rating_partitions = new Partition[epochs.size()];
	for (int e = 0; e < epochs.size(); e++) {
	  rating_partitions[e] = data.getPartition(Integer.toString(cur_partition++));
		inserter = data.getInserter(rating, rating_partitions[e]);
		InserterUtils.loadDelimitedDataTruth(inserter, ratingsPath + "jester-" + num_users + "-te-obs-" + sequence + "-" + e + ".txt");
		inserter = data.getInserter(ratingObs, rating_partitions[e]);
		InserterUtils.loadDelimitedDataTruth(inserter, ratingsPath + "jester-" + num_users + "-te-obs-" + sequence + "-" + e + ".txt");
	}
		

	/** POPULATE TRAINING DATA ***/

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
	AdjCosineSimilarity userCosSim = new AdjCosineSimilarity(ratingObs, 1, avgJokeRatingObs, simThresh);
	AdjCosineSimilarity jokeCosSim = new AdjCosineSimilarity(ratingObs, 0, avgUserRatingObs, simThresh);

	/* Precompute average ratings. */
	log.info("Computing averages ...")
	Database trainDB = data.getDatabase(read_tr, joke_part, user_part_tr);
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
	trainDB = data.getDatabase(write_tr, toClose, read_tr, joke_part, user_part_tr);
	dbPop = new DatabasePopulator(trainDB);
	dbPop.populate(new QueryAtom(rating, User, Joke), subs);
	Database labelsDB = data.getDatabase(labels_tr, [rating] as Set)

	
	/** TRAINING **/
	
	log.info("Starting training ...");
	ConfigBundle config = configs.get(0);
	def configName = config.getString("name", "");
	def method = config.getString("learningmethod", "");
	WeightLearner.learn(method, m, trainDB, labelsDB, initWeights, config, log)
	log.info("Learned model {}: \n {}", configName, m.toString())
	trainDB.close()
	
	
	/** ONLINE INFERENCE ON TEST SET **/
	
	log.info("Starting online inference ...");
	for (int epoch = 0; epoch < epochs.size(); epoch++) {
		
		log.info("Starting epoch {}", epoch);
		
		/* Create new test partitions. (Wasteful, but I don't see a way around it.) */
		read_te = data.getPartition(Integer.toString(cur_partition++));
		write_te = data.getPartition(Integer.toString(cur_partition++));
		
		/* Combine observations from current epoch. */
		def read_partition_list = new ArrayList<Partition>();
		read_partition_list.add(joke_part);
		read_partition_list.add(user_part_te);
		for (int e = 0; e <= epoch; e++) {
			read_partition_list.add(rating_partitions[e]);
		}
		
		/* Clear the users, jokes so we can reuse */
		users.clear();
		jokes.clear();
	
		/* Create an atom for the prior (we'll overwrite later). */
		data.getInserter(ratingPrior, read_te).insertValue(0.5, constant);

		/* Get the test set users/jokes and precompute averages. */
		log.info("Computing averages ...")
		Database testDB = data.getDatabase(read_te, read_partition_list.toArray(new Partition[0]));
		results = testDB.executeQuery(Queries.getQueryForAllAtoms(user));
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
		avgAllRatingObs = 0.0;
		allRatingObs = Queries.getAllAtoms(testDB, ratingObs);
		for (GroundAtom a : allRatingObs) {
			avgAllRatingObs += a.getValue();
		}
		avgAllRatingObs /= allRatingObs.size();
		log.info("  Average rating (test): {}", avgAllRatingObs);
		priorAtom = (RandomVariableAtom) testDB.getAtom(ratingPrior, constant);
		priorAtom.setValue(avgAllRatingObs);
	
		/* Precompute the similarities. */
		log.info("Computing testing similarities ...")
		nnzSim = 0;
		avgsim = 0.0;
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
	
		/* Populate testing database. */
		log.info("Populating testing database ...");
		toClose = [user,joke,ratingObs,ratingPrior,avgUserRatingObs,avgJokeRatingObs,simObsRating] as Set;
		testDB = data.getDatabase(write_te, toClose, read_partition_list.toArray(new Partition[0]));
		dbPop = new DatabasePopulator(testDB);
		dbPop.populate(new QueryAtom(rating, User, Joke), subs);
		testDB.close();
	
		/* Inference on test set */
		Database predDB = data.getDatabase(write_te, toClose, read_partition_list.toArray(new Partition[0]));
		Set<GroundAtom> allAtoms = Queries.getAllAtoms(predDB, rating)
		for (RandomVariableAtom atom : Iterables.filter(allAtoms, RandomVariableAtom)) {
			atom.setValue(0.0);
		}
		MPEInference mpe = new MPEInference(m, predDB, config)
		FullInferenceResult result = mpe.mpeInference()
		log.info("Objective: {}", result.getTotalWeightedIncompatibility());
		def outFilename = outPath + "jester-" + num_users + "-preds-" + sprintf('%02d',sequence) + "-" + sprintf('%02d',epoch) + ".out";
		log.info("Writing to file: {}", outFilename);
		DataOutputter.outputPredicate(outFilename, predDB, rating, "\t", true, null);
		predDB.close();
	
	}

}
