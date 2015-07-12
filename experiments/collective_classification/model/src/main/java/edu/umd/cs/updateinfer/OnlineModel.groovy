package edu.umd.cs.updateinfer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.psl.groovy.*;
import edu.umd.cs.psl.config.*;
import edu.umd.cs.psl.core.*;
import edu.umd.cs.psl.core.inference.*;
import edu.umd.cs.psl.ui.loading.*
import edu.umd.cs.psl.evaluation.result.*;


import edu.umd.cs.psl.database.DataStore;
import edu.umd.cs.psl.database.Database;
import edu.umd.cs.psl.database.DatabasePopulator;
import edu.umd.cs.psl.database.DatabaseQuery;
import edu.umd.cs.psl.database.Partition;
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore;
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver;
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type;


import edu.umd.cs.psl.application.inference.MPEInference;
import edu.umd.cs.psl.application.inference.LazyMPEInference;
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
import java.util.HashSet;

import groovy.time.*;

class OnlineModel { 
  Logger log = LoggerFactory.getLogger(this.class);

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
  
  def inferTestVars(data, u, m, perturb, epoch){
    Date teStart = new Date();
    log.info("STATUS: Starting inference on test set for epoch "+epoch);
    ConfigManager cm = ConfigManager.getManager();
    ConfigBundle mpeBundle = cm.getBundle("mpe");
    HashSet inferClosedPreds = new HashSet<StandardPredicate>([Link,Evidence]);
    Database teInferDB = data.getDatabase(data.getPartition("test_targets"+u.makePerturbEpochStr(perturb, epoch)), inferClosedPreds, data.getPartition("test_edges"),data.getPartition("test_observations"+u.makePerturbEpochStr(perturb, epoch)),data.getPartition("test_evidence"+u.makePerturbEpochStr(perturb, epoch)));

    
    def mpe = new MPEInference(m, teInferDB, mpeBundle);
    def result = mpe.mpeInference();
    teInferDB.close();

    Date stop = new Date();
    TimeDuration td = TimeCategory.minus( stop, teStart );
    
    log.info("STATUS: Finished inference on test set (epoch "+epoch+") in "+td);
  }
  
  
  static void main(args){
    def filePrefix = args[0];
    def perturbs = args[1].toInteger();
    def epochs = args[2].toInteger();
    Date start = new Date();
    Utils u = new Utils();
    OnlineModel om = new OnlineModel();

    DataStore data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, './psl', false), new EmptyBundle());
    PSLModel m = new PSLModel(om, data);
    def defaultWt = 100;
    def weightMap = ["Link":defaultWt,
		     "Evidence":defaultWt,
		     "Prior":defaultWt];
		  
    m = om.addRules(m, weightMap);
    m = om.learnWeights(data, u, m);
    PSLModel um = new PSLModel(u, data);
    def epochStr, perturbStr; 
    for(def perturb=0; perturb < perturbs; perturb++){
      for(def epoch=0; epoch <= epochs; epoch++){
      perturbStr = u.formatInteger(perturb);
      epochStr = u.formatInteger(epoch);
      om.inferTestVars(data,u,m,perturbStr,epochStr);
      u.write_results(data, data.getPartition("test_targets"+u.makePerturbEpochStr(perturbStr, epochStr)), filePrefix+"."+perturbStr+"."+epochStr+".out");
      }
    }
    Date stop = new Date();
    TimeDuration td = TimeCategory.minus( stop, start );
    om.log.info("online model script finished in "+td)
    data.close();
  }

}