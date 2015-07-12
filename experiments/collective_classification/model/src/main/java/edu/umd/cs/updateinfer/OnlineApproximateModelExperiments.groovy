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

class OnlineApproximateModelExperiments { 
	Logger log = LoggerFactory.getLogger(this.class);


	static void main(args){
		def filePrefix = args[0];
		def perturbs = args[1].toInteger();
		def epochs = args[2].toInteger();
		def actMethod = args[3];
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

		def epochStr, perturbStr; 
		ConfigManager cm = ConfigManager.getManager();
		ConfigBundle cfg = cm.getBundle("mpe");

		def actPcts = [0.5] as Set;
		def method = actMethod;
		for (def actPct : actPcts){
			Date start = new Date();
			cfg.setProperty("activator.scoring_method", method);
			cfg.setProperty("onlinempeinference.activation_percent", actPct);
			def outPfx = filePrefix+method+"_"+actPct;
			om.log.info("Running model with method "+method+" and activating "+actPct);
			for(def perturb=0; perturb < perturbs; perturb++){
				ADMMStateActivator activator = null;
				Database prevDB = null;
				def first = true;
				for(def epoch=0; epoch <= epochs; epoch++){
					perturbStr = u.formatInteger(perturb);
					epochStr = u.formatInteger(epoch);

					def infRes = om.inferTestVars(data,u,m,perturbStr,epochStr,prevDB,activator,first,cfg);
					prevDB = infRes[0]; activator = infRes[1];
					if(first){
						first = false;
						u.write_results(data, data.getPartition("test_targets"+u.makePerturbEpochStr(perturbStr, epochStr)), outPfx+"."+perturbStr+"."+epochStr+".out");
					} else {
						u.write_results(data, data.getPartition("test_targets_clean"+u.makePerturbEpochStr(perturbStr, epochStr)), outPfx+"."+perturbStr+"."+epochStr+".out");
						data.deletePartition(data.getPartition("test_targets_clean"+u.makePerturbEpochStr(perturbStr, epochStr)));
					}
				} //epochs
				prevDB.close();
			} //sequences
			Date stop = new Date();
			TimeDuration td = TimeCategory.minus( stop, start );
			om.log.info("online model script "+method+" finished in "+td)
		}
		data.close();
	}
}