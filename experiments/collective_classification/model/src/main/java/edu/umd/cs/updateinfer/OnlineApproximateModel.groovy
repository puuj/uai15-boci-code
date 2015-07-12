package edu.umd.cs.updateinfer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.psl.groovy.*;
import edu.umd.cs.psl.config.*;
import edu.umd.cs.psl.core.*;
import edu.umd.cs.psl.core.inference.*;
import edu.umd.cs.psl.reasoner.admm.ADMMStateActivator
import edu.umd.cs.psl.ui.loading.*
import edu.umd.cs.psl.evaluation.result.*;


import edu.umd.cs.psl.database.DataStore;
import edu.umd.cs.psl.database.Database;
import edu.umd.cs.psl.database.DatabasePopulator;
import edu.umd.cs.psl.database.DatabaseQuery;
import edu.umd.cs.psl.database.Partition;
import edu.umd.cs.psl.database.rdbms.DatabaseComparer
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore;
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver;
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type;


import edu.umd.cs.psl.application.inference.MPEInference;
import edu.umd.cs.psl.application.inference.OnlineMPEInference;
import edu.umd.cs.psl.application.learning.weight.*;
import edu.umd.cs.psl.application.learning.weight.maxlikelihood.*;
import edu.umd.cs.psl.application.learning.weight.maxmargin.*;

import edu.emory.mathcs.utils.ConcurrencyUtils;

import edu.umd.cs.psl.model.argument.ArgumentType
import edu.umd.cs.psl.model.predicate.Predicate
import edu.umd.cs.psl.model.predicate.StandardPredicate
import edu.umd.cs.psl.model.argument.GroundTerm
import edu.umd.cs.psl.model.argument.Variable
import edu.umd.cs.psl.model.atom.*

import edu.umd.cs.updateinfer.*

import java.io.*;
import java.util.*;

import groovy.time.*;

class OnlineApproximateModel { 
	Logger log = LoggerFactory.getLogger(this.class);
	boolean printlog = true;
	
	def addRules(m, weightMap){
		m.add PredicateConstraint.Functional , on : Label
		def sqPotentials = true;

		m.add rule: ( Link(A,B) & Label(A,X) ) >> Label(B,X),
		squared: sqPotentials,
		weight : weightMap["Link"];

		m.add rule: ( Evidence(A,X) ) >> Label(A,X),
		squared: sqPotentials,
		weight : weightMap["Evidence"];

		m.add rule: ~Label(A,X),
		squared: sqPotentials,
		weight : weightMap["Prior"];

		return m;
	}

	def learnWeights(data, u, m){
		/*** Perform weight learning to learn weights that approximate inference results ***/
		log.info("STATUS: Starting weight learning");
		Date wlStart = new Date();

		ConfigManager cm = ConfigManager.getManager();
		ConfigBundle wlBundle = cm.getBundle("wl");
		def perturb = u.formatInteger(0);
		def epoch = u.formatInteger(0);

		HashSet learnClosedPreds = new HashSet<StandardPredicate>([Link,Evidence]);
		HashSet labelClosedPreds = new HashSet<StandardPredicate>([Label,Link,Evidence]);

		Database wlLearnDB = data.getDatabase(data.getPartition("train_targets"+u.makePerturbEpochStr(perturb, epoch)), learnClosedPreds, data.getPartition("train_edges"),data.getPartition("train_observations"+u.makePerturbEpochStr(perturb, epoch)),data.getPartition("train_evidence"+u.makePerturbEpochStr(perturb, epoch)));
		Database wlLabelDB = data.getDatabase(data.getPartition("train_truth"+u.makePerturbEpochStr(perturb, epoch)), labelClosedPreds, data.getPartition("train_edges"),data.getPartition("train_observations"+u.makePerturbEpochStr(perturb, epoch)),data.getPartition("train_evidence"+u.makePerturbEpochStr(perturb, epoch)));

		VotedPerceptron vp = new MaxLikelihoodMPE(m, wlLearnDB, wlLabelDB, wlBundle);
		vp.learn();

		wlLearnDB.close();
		wlLabelDB.close();

		Date stop = new Date();
		TimeDuration td = TimeCategory.minus( stop, wlStart );

		log.info("STATUS: Finished weight learning in "+td);
		log.info("Learned Model:\n"+m);
		return m;
	}

	def inferTestVars(data, u, m, perturb, epoch, prevDB, activator, first, cfg){
		Date teStart = new Date();
		log.info("STATUS: Starting inference on test set for epoch "+epoch);
		ConfigManager cm = ConfigManager.getManager();
		ConfigBundle mpeBundle = cfg;//
		HashSet inferClosedPreds = new HashSet<StandardPredicate>([Link,Evidence]);
		Database teInferDB;
		if(first){
			teInferDB = data.getDatabase(data.getPartition("test_targets"+u.makePerturbEpochStr(perturb, epoch)), inferClosedPreds, data.getPartition("test_edges"),data.getPartition("test_observations"+u.makePerturbEpochStr(perturb, epoch)),data.getPartition("test_evidence"+u.makePerturbEpochStr(perturb, epoch)));
		} else {
			teInferDB = data.getDatabase(data.getPartition("test_targets_clean"+u.makePerturbEpochStr(perturb, epoch)), inferClosedPreds, data.getPartition("test_edges"),data.getPartition("test_observations"+u.makePerturbEpochStr(perturb, epoch)),data.getPartition("test_evidence"+u.makePerturbEpochStr(perturb, epoch)));
		}
		Database teReadDB = data.getDatabase(data.getPartition("dummy_"+epoch), inferClosedPreds, data.getPartition("test_edges"),data.getPartition("test_observations"+u.makePerturbEpochStr(perturb, epoch)),data.getPartition("test_evidence"+u.makePerturbEpochStr(perturb, epoch)));
		Set<QueryAtom> atomsToConsider = new HashSet<QueryAtom>();
		if(prevDB != null){
			DatabaseComparer dbc = new DatabaseComparer(data,prevDB,teReadDB);
//			for( StandardPredicate p : new ArrayList([Evidence, Label, Link]) ){ 
			for( StandardPredicate p : new ArrayList([Label]) ){ 
				atomsToConsider.addAll(dbc.queryUpdatedAtoms(p));
				atomsToConsider.addAll(dbc.queryAddedAtoms(p));
				atomsToConsider.addAll(dbc.queryRemovedAtoms(p));
			}    
		}
		
		def mpe = new OnlineMPEInference(m, teInferDB, mpeBundle, activator, atomsToConsider);
		def result = mpe.mpeInference();
		teInferDB.close();
		if(prevDB!=null){prevDB.close();}

		Date stop = new Date();
		TimeDuration td = TimeCategory.minus( stop, teStart );

		log.info("STATUS: Finished inference on test set (seq-epoch "+perturb+"-"+epoch+") in "+td);
		return [teReadDB, new ADMMStateActivator(result.getADMMReasonerState(), mpeBundle)];
		//return [null, null]; //Uncomment to force full inference
	}

	static void main(args){
		def filePrefix = args[0];
		def perturbs = args[1].toInteger();
		def epochs = args[2].toInteger();
		Date start = new Date();
		Utils u = new Utils();
		OnlineApproximateModel om = new OnlineApproximateModel();

		DataStore data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, './psl', false), new EmptyBundle());
		PSLModel m = new PSLModel(om, data);
		def defaultWt = 100;
		def weightMap = ["Link":defaultWt,
		                 "Functional":defaultWt,
		                 "Evidence":defaultWt,
		                 "Prior":defaultWt];

		m = om.addRules(m, weightMap);
		m = om.learnWeights(data, u, m);
		PSLModel um = new PSLModel(u, data);
		
		double actStep = 0.75/(double)epochs;
		def epochStr, perturbStr; 
		ConfigManager cm = ConfigManager.getManager();
		ConfigBundle cfg = cm.getBundle("mpe");

		for(def perturb=0; perturb < perturbs; perturb++){
			ADMMStateActivator activator = null;
			Database prevDB = null;
			def first = true;
			double actPct = 0.90;

			for(def epoch=0; epoch <= epochs; epoch++){
				if(om.printlog) { println "PSEQ__"+perturb+"\tEPOCH__"+epoch; }
				perturbStr = u.formatInteger(perturb);
				epochStr = u.formatInteger(epoch);

				def infRes = om.inferTestVars(data,u,m,perturbStr,epochStr,prevDB,activator,first,cfg);
				prevDB = infRes[0]; activator = infRes[1];
				if(first){
					first = false;
					u.write_results(data, data.getPartition("test_targets"+u.makePerturbEpochStr(perturbStr, epochStr)), filePrefix+"."+perturbStr+"."+epochStr+".out");
				} else {
					//u.write_results(data, data.getPartition("test_targets"+u.makePerturbEpochStr(perturbStr, epochStr)), filePrefix+"."+perturbStr+"."+epochStr+".out");
					u.write_results(data, data.getPartition("test_targets_clean"+u.makePerturbEpochStr(perturbStr, epochStr)), filePrefix+"."+perturbStr+"."+epochStr+".out");
					data.deletePartition(data.getPartition("test_targets_clean"+u.makePerturbEpochStr(perturbStr, epochStr)));
				}
				if(true){ println "==END=="; }
			}
			if(prevDB!=null){prevDB.close();}
		}
		Date stop = new Date();
		TimeDuration td = TimeCategory.minus( stop, start );
		om.log.info("online model script finished in "+td)
		data.close();
	}
}