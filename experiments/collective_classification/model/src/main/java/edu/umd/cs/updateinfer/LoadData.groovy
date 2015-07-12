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

class LoadData { 
  Logger log = LoggerFactory.getLogger(this.class);

  def load_data_perturb_epoch(datastore, utils, dataroot, filePrefix, partitionPrefix, trial, perturb, epoch){
    log.info("loading "+partitionPrefix+" data for perturb-"+perturb+" epoch-"+epoch); 
    def predMap;
    /*** Load inference targets for training set ***/
    Partition targets = datastore.getPartition(partitionPrefix+"_targets"+utils.makePerturbEpochStr(perturb,epoch));
    predMap = [((Predicate)Label):dataroot+filePrefix+"."+partitionPrefix+"."+trial+"."+perturb+"."+epoch+".targets.txt"];
    utils.loadPredicateAtoms(datastore, predMap, targets)

    /*** Load label evidence for training set ***/
    Partition ev = datastore.getPartition(partitionPrefix+"_evidence"+utils.makePerturbEpochStr(perturb,epoch));
    predMap = [((Predicate)Evidence):dataroot+filePrefix+"."+partitionPrefix+"."+trial+"."+perturb+"."+epoch+".evidence.txt"];
    utils.loadPredicateAtomsWithValue(datastore, predMap, ev)

    /*** Load true labels for training set ***/
    Partition truth = datastore.getPartition(partitionPrefix+"_truth"+utils.makePerturbEpochStr(perturb,epoch));
    predMap = [((Predicate)Label):dataroot+filePrefix+"."+partitionPrefix+"."+trial+"."+perturb+"."+epoch+".truth.txt"];
    utils.loadPredicateAtomsWithValue(datastore, predMap, truth)

    /*** Load observed node labels ***/
    Partition obs = datastore.getPartition(partitionPrefix+"_observations"+utils.makePerturbEpochStr(perturb,epoch));
    predMap = [((Predicate)Label):dataroot+filePrefix+"."+partitionPrefix+"."+trial+"."+perturb+"."+epoch+".obs.txt"];
    utils.loadPredicateAtomsWithValue(datastore, predMap, obs) ;

}


  def load_data_trial(datastore, utils, dataroot, filePrefix, partitionPrefix, trial){
    log.info("loading "+partitionPrefix+" data"); 
    def predMap;
    /*** Load edges ***/
    Partition edges = datastore.getPartition(partitionPrefix+"_edges");
    predMap = [((Predicate)Link):dataroot+filePrefix+"."+partitionPrefix+"."+trial+".edges.txt"];
    utils.loadPredicateAtoms(datastore, predMap, edges)

    /*** Load observed node labels 
    Partition obs = datastore.getPartition(partitionPrefix+"_observations");
    predMap = [((Predicate)Label):dataroot+filePrefix+"."+partitionPrefix+"."+trial+".obs.txt"];
    utils.loadPredicateAtomsWithValue(datastore, predMap, obs) ***/
  }

static void main(args){
    Date start = new Date();
    Utils u = new Utils();

    //Where the data resides (first argument to this script)
    def dataroot = args[0];
    def trial = args[1].toInteger();
    def perturbs = args[2].toInteger();
    def epochs = args[3].toInteger();
    def filePrefix = "updateinfer_er";
    def predMap;

    LoadData ld = new LoadData();
    ld.log.info("initializing DB and model")

    DataStore data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, './psl', true), new EmptyBundle());
    PSLModel m = new PSLModel(ld, data);
    m = u.definePredicates(m);
    def epoch = 0; def perturb = 0;

    def trialStr = u.formatInteger(trial);
    def perturbStr = u.formatInteger(perturb);
    def epochStr = u.formatInteger(epoch);

    ld.load_data_trial(data,u,dataroot,filePrefix,"train",trialStr);
    ld.load_data_perturb_epoch(data,u,dataroot,filePrefix,"train",trialStr, perturbStr, epochStr);

    ld.load_data_trial(data,u,dataroot,filePrefix,"test",trialStr);
    
    for(perturb=0; perturb < perturbs; perturb++){ 
      for(epoch=0; epoch <= epochs; epoch++){
	epochStr = u.formatInteger(epoch);
	perturbStr = u.formatInteger(perturb);
	ld.load_data_perturb_epoch(data,u,dataroot,filePrefix,"test",trialStr,perturbStr,epochStr);
      }
    }

    Date stop = new Date();

    TimeDuration td = TimeCategory.minus( stop, start );
    ld.log.info("data loading finished in "+td)
    data.close();
  }
} 