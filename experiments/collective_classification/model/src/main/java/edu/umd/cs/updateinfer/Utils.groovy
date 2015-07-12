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

import edu.umd.cs.psl.model.predicate.*
import edu.umd.cs.psl.model.argument.*
import edu.umd.cs.psl.model.atom.*

import edu.umd.cs.psl.util.database.Queries;

import edu.umd.cs.psl.evaluation.debug.AtomPrinter;
import edu.umd.cs.psl.evaluation.resultui.printer.AtomPrintStream;
import edu.umd.cs.psl.evaluation.resultui.printer.DefaultAtomPrintStream;


import edu.umd.cs.psl.online_cc.*

import java.io.*;
import java.util.*;
import java.util.HashSet;

import groovy.time.*;

class Utils { 

  Date start = new Date();

  Logger log = LoggerFactory.getLogger(this.class);


  def formatInteger(int num){
    return sprintf('%03d',num);
  }

  def makePerturbEpochStr(perturb, epoch){
    return "_"+perturb+"_"+epoch;
  }
  
  def loadPredicateAtoms(datastore, predicateMap, targetPartition){
    for (Predicate p : predicateMap.keySet() ){
      log.debug("Loading files "+predicateMap[p]);
      InserterUtils.loadDelimitedData(datastore.getInserter(p,targetPartition),predicateMap[p]);
    }
  }
    
  def loadPredicateAtomsWithValue(datastore, predicateMap, targetPartition){
    for (Predicate p : predicateMap.keySet() ){
      log.debug("Loading files "+predicateMap[p]);
      InserterUtils.loadDelimitedDataTruth(datastore.getInserter(p,targetPartition),predicateMap[p]);
    }
  }

  def definePredicates(model) { 
    /*** Target Predicates ***/
    model.add predicate: "Label", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]

    model.add predicate: "Evidence", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]

    model.add predicate: "Link", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]

    return model;
  }

  def write_results(datastore, readPartition, filename){
    Partition dummy = datastore.getNewPartition();
    Database resultsDB = datastore.getDatabase(dummy, readPartition);
    def outFile = new PrintWriter(filename);
    Set atomSet = Queries.getAllAtoms(resultsDB,Label);
    for (Atom a : atomSet) {
      def first = true;
      for (Term t : a.getArguments() ) {
	if(first) { outFile.print(t.toString()); first = false}
	else { outFile.print("\t"+t.toString()) }
      }
      if(a instanceof edu.umd.cs.psl.model.atom.GroundAtom){
	outFile.print("\t"); outFile.format("%3.2f",a.getValue()); 
      }
      outFile.print("\n");
    }
    outFile.close()
    resultsDB.close();
    datastore.deletePartition(dummy);
  }

  def print_results(datastore, readPartition){
    Partition dummy = datastore.getNewPartition();
    Database resultsDB = datastore.getDatabase(dummy, readPartition);
    
    AtomPrintStream printer = new DefaultAtomPrintStream();
    Set atomSet = Queries.getAllAtoms(resultsDB,Label);
    for (Atom a : atomSet) {
      def first = true;
      for (Term t : a.getArguments() ) {
	if(first) { System.out.print(t.toString()); first = false}
	else { System.out.print("\t"+t.toString()) }
      }
      if(a instanceof edu.umd.cs.psl.model.atom.GroundAtom){
	System.out.print("\t"); System.out.format("%3.2f",a.getValue()); 
      }
      System.out.print("\n");
      //        printer.printAtom(a);
    }

    resultsDB.close();
    datastore.deletePartition(dummy);
  }

}