/**
 * This file is part of the PSL software.
 * Copyright 2011-2014 University of Maryland
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umd.cs.psl.application.inference;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.psl.application.util.GroundKernels;
import edu.umd.cs.psl.application.util.Grounding;
import edu.umd.cs.psl.config.ConfigBundle;
import edu.umd.cs.psl.config.ConfigManager;
import edu.umd.cs.psl.config.Factory;
import edu.umd.cs.psl.database.Database;
import edu.umd.cs.psl.database.DatabasePopulator;
import edu.umd.cs.psl.database.rdbms.RDBMSDatabase;
import edu.umd.cs.psl.evaluation.result.FullInferenceResult;
import edu.umd.cs.psl.evaluation.result.memory.ReasonerFullInferenceResult;
import edu.umd.cs.psl.model.Model;
import edu.umd.cs.psl.model.argument.GroundTerm;
import edu.umd.cs.psl.model.atom.Atom;
import edu.umd.cs.psl.model.atom.AtomEvent;
import edu.umd.cs.psl.model.atom.AtomEventFramework;
import edu.umd.cs.psl.model.atom.AtomManager;
import edu.umd.cs.psl.model.atom.GroundAtom;
import edu.umd.cs.psl.model.atom.LazyPersistedAtomManager;
import edu.umd.cs.psl.model.atom.ObservedAtom;
import edu.umd.cs.psl.model.atom.PersistedAtomManager;
import edu.umd.cs.psl.model.atom.QueryAtom;
import edu.umd.cs.psl.model.atom.RandomVariableAtom;
import edu.umd.cs.psl.model.kernel.GroundKernel;
import edu.umd.cs.psl.model.kernel.Kernel;
import edu.umd.cs.psl.model.kernel.predicateconstraint.DomainRangeConstraintKernel;
import edu.umd.cs.psl.model.kernel.predicateconstraint.GroundValueConstraint;
import edu.umd.cs.psl.model.kernel.predicateconstraint.SymmetryConstraintKernel;
import edu.umd.cs.psl.model.kernel.predicateconstraint.ValueConstraintKernel;
import edu.umd.cs.psl.reasoner.Reasoner;
import edu.umd.cs.psl.reasoner.ReasonerFactory;
import edu.umd.cs.psl.reasoner.admm.ADMMReasoner;
import edu.umd.cs.psl.reasoner.admm.ADMMReasonerFactory;
import edu.umd.cs.psl.reasoner.admm.ADMMReasonerState;
import edu.umd.cs.psl.reasoner.admm.ADMMReasonerState.ADMMVariableEntry;
import edu.umd.cs.psl.reasoner.admm.ADMMStateActivator;
import edu.umd.cs.psl.reasoner.admm.StreamingADMMReasoner;
import edu.umd.cs.psl.reasoner.function.AtomFunctionVariable;
import edu.umd.cs.psl.reasoner.function.MutableAtomFunctionVariable;
import edu.umd.cs.psl.util.collection.MapSampler;
import edu.umd.cs.psl.util.collection.MapSorter;

/**
 * @author jay
 *
 */
public class OnlineMPEInference extends MPEInference {
private static final Logger log = LoggerFactory.getLogger(OnlineMPEInference.class);
	
	/**
	 * Prefix of property keys used by this class.
	 * 
	 * @see ConfigManager
	 */
	public static final String CONFIG_PREFIX = "onlinempeinference";
	
	/**
	 * Key for {@link Factory} or String property.
	 * <p>
	 * Should be set to a {@link ReasonerFactory} or the fully qualified
	 * name of one. Will be used to instantiate a {@link Reasoner}.
	 */
	public static final String REASONER_KEY = CONFIG_PREFIX + ".reasoner";
	/**
	 * Default value for REASONER_KEY.
	 * <p>
	 * Value is instance of {@link ADMMReasonerFactory}. 
	 */
	public static final ReasonerFactory REASONER_DEFAULT = new ADMMReasonerFactory();
	
	public static final String ACTIVATION_QUOTA = CONFIG_PREFIX + ".activation_quota";
	public static final String ACTIVATION_PERCENT = CONFIG_PREFIX + ".activation_percent";
	public static final String SAMPLED_ACTIVATION = CONFIG_PREFIX + ".sampled_activation";
	public static final String RELATIONAL_ACTIVATION = CONFIG_PREFIX + ".relational_activation";
	
	private Model model;
	private Database db;
	private ConfigBundle config;
	private ADMMStateActivator activator;
	private Set<QueryAtom> toActivate;
	
	public OnlineMPEInference(Model model, Database db, ConfigBundle config, ADMMStateActivator activator, Set<QueryAtom> atomsToActivate) {
		super(model,db,config);
		this.model = model;
		this.db = db;
		this.toActivate = atomsToActivate;
		this.config = config;
		this.activator = activator;
	}

	private Set<GroundAtom> activateModel(Reasoner reasoner, 
			AtomManager atomManager, AtomEventFramework eventFramework){
		Set<GroundAtom> activated = new HashSet<GroundAtom>();
		
		/** First activate the changes requested by model */
		log.info("Activating atoms from model - "+toActivate.size()+" atoms");
		for(Atom q : toActivate){
			GroundAtom a = atomManager.getAtom(q.getPredicate(), (GroundTerm[]) q.getArguments());
			activated.add(a);
			if(a instanceof RandomVariableAtom){
				log.trace("Activated - "+a.toString()+" as a variable");
				eventFramework.activateAtom((RandomVariableAtom) a, AtomEvent.Type.ActivatedRVAtomOnline);
			} else if(a instanceof ObservedAtom){
				log.trace("Activated - "+a.toString()+" as an observation");
				eventFramework.activateAtom((ObservedAtom) a);
			}
		}
		
		//Score the variables
		Map<GroundAtom,Double> varScores = activator.scoreVariables();
		int activatedCnt = activated.size();
		int activateQuota = computeActivationQuota(varScores.size());
		if(config.getBoolean(RELATIONAL_ACTIVATION, false)){
			activated = exploreBFSTiebreaking(atomManager, eventFramework, activated, varScores, activateQuota);
		} else if(config.getBoolean(SAMPLED_ACTIVATION, false)){
			Set<GroundAtom> sampledActivation =  MapSampler.sampleRepresentativeSet(varScores, activateQuota);
			for(GroundAtom var : sampledActivation){
				if(activatedCnt >= activateQuota){ break; }
				activatedCnt += activateVariable(var, varScores.get(var), activated, atomManager, eventFramework);
			}			
		} else {
			Map<GroundAtom, Double> sortVarScores = MapSorter.sortByValueDescending(varScores);
			for(GroundAtom var : sortVarScores.keySet()){
				if(activatedCnt >= activateQuota){ break; }
				activatedCnt += activateVariable(var, varScores.get(var), activated, atomManager, eventFramework);
			}
		} 
		return activated;

	}

	/** Explore atom BFS - score-based tie breaking
	 * Given this atom, perform BFS and add all atoms to a list until the quota
	 * is exceeded. Use variable scores to order the atoms in the BFS queue
	 ***/
	private Set<GroundAtom> exploreBFSTiebreaking(AtomManager atomManager, AtomEventFramework eventFramework, 
			Set<GroundAtom> activated, Map<GroundAtom,Double> varScores, int quota){
		Queue<AtomPriorityQueueElement> frontier = new PriorityQueue<AtomPriorityQueueElement>();
		Map<GroundAtom,AtomPriorityQueueElement> explored = new HashMap<GroundAtom,AtomPriorityQueueElement>();
		int selected = activated.size();
		
		//First add all the 1-hop neighbors of seeds
		Set<GroundAtom> seedNeighbors = new HashSet<GroundAtom>();
		for(GroundAtom seed : activated){
			seedNeighbors.addAll(getRelatedAtoms(seed));
		}
		for( GroundAtom rel : seedNeighbors){
			if(varScores.containsKey(rel)){
				AtomPriorityQueueElement apqe = new AtomPriorityQueueElement(rel, varScores.get(rel), 1);
				log.trace("Adding atom (from seedNeighbors) to frontier "+rel.toString()+" with score "+formatDouble(apqe.getScore()));
				explored.put(rel, apqe);		
				frontier.add(apqe);
			} else {
				log.trace("Skipping atom without score "+rel.toString());
			}
		}
		
		//Now do the BFS for real
		AtomPriorityQueueElement seed;
		while(selected < quota && (seed = frontier.poll()) != null) {
			GroundAtom seedAtom = seed.getAtom();
			selected += activateVariable(seedAtom, varScores.get(seedAtom), activated, atomManager, eventFramework);
			log.trace("Activating variable for "+seedAtom.toString()+" with scores "+formatDouble(varScores.get(seedAtom))+"/"+formatDouble(seed.getScore())+" results in "+selected+" total selected");
			for( GroundAtom rel : getRelatedAtoms(seedAtom)){
				if(varScores.containsKey(rel)){
					AtomPriorityQueueElement apqe =  new AtomPriorityQueueElement(rel, varScores.get(rel), seed.getDistance()+1);
					if(explored.containsKey(rel)){ //seen earlier, possibly closer now?
						if(apqe.getDistance() < explored.get(rel).getDistance()){
							log.trace("Replacing previously-seen but closer atom (from frontierBFS) to frontier "+rel.toString()+" with score "+formatDouble(apqe.getScore()));
							frontier.remove(explored.get(rel));
							explored.remove(rel);
							explored.put(rel,apqe);
							frontier.add(apqe);
						} //closer
					} else {
						log.trace("Adding (unseen) atom (from frontierBFS) to frontier "+rel.toString()+" with score "+formatDouble(apqe.getScore()));
						frontier.add(apqe);
						explored.put(rel, apqe);
					}
				} else {
					log.trace("Skipping atom without score (frontier) "+rel.toString());
				}//scored variable
			} //related atoms
		} //frontier exploration
		
		return activated;
	}


	
	private String formatDouble(Double d){
		return String.format("%03.3f",d);
	}
	
	
	private int computeActivationQuota(double numVars){
	//Figure out how much to activate
			double activatePct = config.getDouble(ACTIVATION_PERCENT, 0);
			int activateQuota = config.getInt(ACTIVATION_QUOTA, 0);
			if(activateQuota > 0 && activatePct > 0){
				log.warn("Both Activation quota and percent are set - which should I use? (using quota of "+activateQuota+")");
				activatePct = 0;
			} else if(activatePct == 0 && activateQuota == 0){
				log.warn("Neither Activation quota or percent are set - (using 25% activation)");
				activatePct = .25;
			}
			if(activatePct > 0){
				activateQuota = (int)(numVars*activatePct);
			}
			return activateQuota;
	}
	
	private int activateVariable(GroundAtom var, double score, Set<GroundAtom> activated,
			AtomManager atomManager, AtomEventFramework eventFramework){
		GroundAtom a = atomManager.getAtom(var.getPredicate(), var.getArguments());
		int activatedCnt = 0;
		//Only activate random variables based on scores
		if(a instanceof RandomVariableAtom){
			if(!activated.contains(a)){ //don't reactivate atoms - wastes quota
				log.trace("Activating atom using Activator score - "+var.toString()+" score="+formatDouble(score));
				eventFramework.activateAtom((RandomVariableAtom) a, AtomEvent.Type.ActivatedRVAtomOnline);
				activatedCnt++;
				activated.add(a);
			}
			ADMMReasonerState state = (activator.getInitialState() == null) ? activator.getState() : activator.getInitialState();
			//even if the atom was already active, still need to check constraints
			for(GroundAtom cg : state.constrainedPairs(var)){
				GroundAtom ca = atomManager.getAtom(cg.getPredicate(), cg.getArguments());
				if(ca instanceof RandomVariableAtom && !activated.contains(ca) ){					
					log.trace("Activating atom due to constraint - "+cg.toString());
					activated.add(ca);
					activatedCnt++;
					eventFramework.activateAtom((RandomVariableAtom) ca, AtomEvent.Type.ActivatedRVAtomOnline);
				}//valid constraint-based activation
			} //for all constrained pairs
		} else { //observed atom
			log.trace("Skipped activating observed atom using Activator score - "+var.toString()+" score="+formatDouble(score));
		}
		return activatedCnt;
	}
	
	private void primeAtomManager(AtomManager atomManager){
		log.info("Priming write partition with values from previous epoch");
		ADMMReasonerState s = activator.getState();
		Map<GroundAtom, ADMMVariableEntry> varStates = s.getVariableStates();
		for(GroundAtom gOld: varStates.keySet()){
			GroundAtom g = atomManager.getAtom(gOld.getPredicate(), gOld.getArguments());
			if(g instanceof RandomVariableAtom){
				((RandomVariableAtom) g).setValue(gOld.getValue());
				((RandomVariableAtom) g).commitToDB();
			}
		}
	}
	
	private Set<GroundAtom> getRelatedAtoms(GroundAtom seed){
		Set<GroundAtom> atoms = new HashSet<GroundAtom>();
		ADMMReasonerState state = (activator.getInitialState() == null) ? activator.getState() : activator.getInitialState(); 
		for (GroundKernel gk : state.getAtomKernels(seed)){
			atoms.addAll(gk.getAtoms());
		}
		return atoms;
	}
	
	
		
	
	private Set<Kernel> findSupportedModelConstraints(){
		Set<Kernel> constraintKernels = new HashSet<Kernel>();
		for( Kernel k : model.getKernels() ){
			if(k instanceof DomainRangeConstraintKernel){
				constraintKernels.add(k);
			}
			if(k instanceof SymmetryConstraintKernel){
				constraintKernels.add(k);
			}

		}
		return constraintKernels;
	}

	private void groundAtomConstraints(GroundAtom atom, Set<Kernel> constraints,
			StreamingADMMReasoner reasoner, AtomManager atomManager){
		for(Kernel k : constraints){
			if(k instanceof DomainRangeConstraintKernel){
				((DomainRangeConstraintKernel) k).groundConstraint(atom, atomManager, reasoner);
			}
			if(k instanceof SymmetryConstraintKernel){
				((SymmetryConstraintKernel) k).groundConstraint(atom, atomManager, reasoner);
			}
		}
	}

	protected void clampVariablesConstraints(StreamingADMMReasoner reasoner, AtomManager atomManager,
			Set<GroundAtom> activated){
		int clamped = 0;
		Set<Kernel> constraintKernels = findSupportedModelConstraints();
		ADMMReasonerState s = activator.getState();
		Map<GroundAtom, ADMMVariableEntry> varStates = s.getVariableStates();
		for(GroundAtom gOld : varStates.keySet()){
			GroundAtom g = atomManager.getAtom(gOld.getPredicate(), gOld.getArguments());
			if(!activated.contains(g) && g instanceof RandomVariableAtom){
				if(varStates.containsKey(gOld)){
					ADMMVariableEntry e = varStates.get(gOld);
					groundAtomConstraints(g,constraintKernels,reasoner,atomManager);
					reasoner.addGroundKernel(new GroundValueConstraint(new ValueConstraintKernel(),g,e.consensusEstimate));
					log.trace("Clamped "+g.toString()+" to consensus value "+formatDouble(e.consensusEstimate));
					clamped++;
				}//consensus known
			}//inference target, not in activated-set
		} //atoms in reasone
		log.info("Clamped "+clamped+" variables to consensus estimates (constraints)");
	}
    
    /**
	 * Minimizes the total weighted incompatibility of the {@link GroundAtom GroundAtoms}
	 * in the Database according to the Model and commits the updated truth
	 * values back to the Database.
	 * <p>
	 * The {@link RandomVariableAtom RandomVariableAtoms} to be inferred are those
	 * persisted in the Database when this method is called. All RandomVariableAtoms
	 * which the Model might access must be persisted in the Database.
	 * 
	 * @return inference results
	 * @see DatabasePopulator
	 */
	public FullInferenceResult mpeInference() 
			throws ClassNotFoundException, IllegalAccessException, InstantiationException {

		Reasoner reasoner = new StreamingADMMReasoner(config);
		PersistedAtomManager atomManager; 
		if(activator != null){
			atomManager = new LazyPersistedAtomManager(db);
			AtomEventFramework eventFramework = new AtomEventFramework(db, config);

			/* Registers the Model's Kernels with the AtomEventFramework */
			for (Kernel k : model.getKernels())
				k.registerForAtomEvents(eventFramework, reasoner);
			log.info("Using Activator to ground model");

			//primeAtomManager(atomManager);
			
			Set<GroundAtom> activatedAtoms = activateModel(reasoner,atomManager, eventFramework);
			log.info("Activated "+activatedAtoms.size()+" total atoms");
			
			eventFramework.workOffJobQueue();
			while (eventFramework.checkToActivate() > 0)
				eventFramework.workOffJobQueue();
			
			clampVariablesConstraints((StreamingADMMReasoner) reasoner,atomManager,activatedAtoms);

		} else {
			atomManager = new PersistedAtomManager(db);
			log.info("Grounding out model.");
			Grounding.groundAll(model, atomManager, reasoner);
		}
		log.info("Beginning inference.");
		reasoner.optimize();
		log.info("Inference complete. Writing results to Database.");

		/* Commits the RandomVariableAtoms back to the Database */
		int count = 0;
		for (RandomVariableAtom atom : atomManager.getPersistedRVAtoms()) {
			log.trace("Commiting to db: "+atom.toString()+" with value "+formatDouble(atom.getValue()));
			atom.commitToDB();
			count++;
		}
		log.debug("Total committed variables: "+count);

		double incompatibility = GroundKernels.getTotalWeightedIncompatibility(reasoner.getCompatibilityKernels());
		double infeasibility = GroundKernels.getInfeasibilityNorm(reasoner.getConstraintKernels());
		int size = reasoner.size();
		return new ReasonerFullInferenceResult(incompatibility, infeasibility, 
				count, size, new ADMMReasonerState((ADMMReasoner) reasoner));
	}


	@Override
	public void close() {
		model=null;
		db = null;
		config = null;
	}
	
	public class AtomPriorityQueueElement implements Comparable<AtomPriorityQueueElement>{

		private GroundAtom a;
		private double score;
		private int dist;
		private double scaledScore = 0;
		
		public AtomPriorityQueueElement(GroundAtom a, double score, int distance) {
			this.a = a; 
			this.score = score;
			this.dist = distance;
			this.scaledScore = computeScore();
		}

		private double computeScore(){
			return score/(Math.pow(2, dist));
		}
		
		public double getScore() { return scaledScore; }
		public int getDistance(){ return dist; }
		public GroundAtom getAtom(){ return a; }


		public boolean equals(Object oth){
			if(oth == null) { return false; }
			if(((AtomPriorityQueueElement)oth) == this) { return true; }
			return a.equals(((AtomPriorityQueueElement)oth).getAtom());
		}

		@Override
		public int compareTo(AtomPriorityQueueElement o) {
			//descending sort by value
			int cmp = Double.compare(o.getScore(), scaledScore);
			if(cmp == 0){
				return a.toString().compareTo(o.getAtom().toString());
			} else {
				return cmp;
			}
		}

	}
	
}
