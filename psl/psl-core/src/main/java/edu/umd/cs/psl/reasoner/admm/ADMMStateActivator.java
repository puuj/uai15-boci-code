/*
 * This file is part of the PSL software.
 * Copyright 2011-2015 University of Maryland
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
package edu.umd.cs.psl.reasoner.admm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.psl.config.ConfigBundle;
import edu.umd.cs.psl.config.ConfigManager;
import edu.umd.cs.psl.model.atom.GroundAtom;
import edu.umd.cs.psl.model.kernel.GroundCompatibilityKernel;
import edu.umd.cs.psl.model.kernel.GroundConstraintKernel;
import edu.umd.cs.psl.model.kernel.GroundKernel;
import edu.umd.cs.psl.reasoner.admm.ADMMObjectiveTerm;
import edu.umd.cs.psl.reasoner.admm.ADMMReasoner;
import edu.umd.cs.psl.reasoner.admm.ADMMReasonerState;
import edu.umd.cs.psl.reasoner.admm.ADMMReasonerState.ADMMVariableEntry;
import edu.umd.cs.psl.reasoner.admm.WeightedObjectiveTerm;

public class ADMMStateActivator {
	private static final Logger log = LoggerFactory.getLogger(ADMMStateActivator.class);

	/**
	 * Prefix of property keys used by this class.
	 * 
	 * @see ConfigManager
	 */
	public static final String CONFIG_PREFIX = "activator";
	
	/**
	 * Key for int property for the maximum number of iterations of ADMM to
	 * perform in a round of inference
	 */
	public static final String SCORING_METHOD = CONFIG_PREFIX + ".scoring_method";
	public static final String INVERT_SCORES = CONFIG_PREFIX + ".invert";
	public static final String PRINT_SCORES = CONFIG_PREFIX + ".print_scores";
	public static final String CONSTRAINT_WEIGHT = CONFIG_PREFIX + ".constraint_weight";
	
	ADMMReasonerState reasonerState;
	ADMMReasonerState initialState = null;
	
	double weightCoeff = 1;
	double lagrangeCoeff = 1;
	private String score_method = "";
	private boolean invert = false;
	private boolean print_scores = false;
	private double constraint_weight = 1000;
	
	public ADMMStateActivator(ADMMReasonerState state, ConfigBundle config) {
		this.reasonerState = state;
		this.score_method = config.getString(SCORING_METHOD, "simple").toLowerCase();
		this.invert = config.getBoolean(INVERT_SCORES,false);
		this.print_scores = config.getBoolean(PRINT_SCORES,false);
		this.constraint_weight = config.getDouble(CONSTRAINT_WEIGHT,1000);
	}

	public ADMMStateActivator(ADMMReasonerState state, ConfigBundle config, ADMMReasonerState origState) {
		this(state,config);
		this.initialState = origState;
	}
	
	public ADMMStateActivator(ADMMReasonerState state, ConfigBundle config, String mode) {
		this(state, config);
		this.score_method = mode.toLowerCase();
	}
	
	public ADMMReasonerState getState(){
		return reasonerState;
	}
	
	public ADMMReasonerState getInitialState(){
		return initialState;
	}
	
	
	private String formatDouble(Double d){
		return String.format("%03.3f",d);
	}
	
	private String doubleListToString(ArrayList<Double> a){
		String ret = "";
		Collections.sort(a);
		for(int i = 0; i < a.size(); i++){
			ret+= formatDouble(a.get(i))+", ";
		}
		return ret;
	}
	private String stringListToString(ArrayList<String> a){
		String ret = "";
		//Collections.sort(a);
		for(int i = 0; i < a.size(); i++){
			ret+= a.get(i)+", ";
		}
		return ret;
	}
		
	private Map<GroundAtom,Double> scoreVariablesTruthValue(){
		HashMap<GroundAtom,Double> varScores= new HashMap<GroundAtom,Double>();
		for(Entry<GroundAtom, ADMMVariableEntry> varInfo : reasonerState.varMap.entrySet() ){
			GroundAtom var = varInfo.getKey();
			String debugStr="";
			debugStr+="Atom "+var.toString()+" with value "+
					formatDouble(var.getValue());
			double score = 1-Math.abs(.5-var.getValue());
			debugStr+=" with final score "+formatDouble(score);
			log.trace(debugStr);
			varScores.put(var, score );
		}
		return varScores;
	}
	
	private Map<GroundAtom,Double> scoreVariablesRandom(){
		HashMap<GroundAtom,Double> varScores= new HashMap<GroundAtom,Double>();
		Random rnd = new Random();
		for(Entry<GroundAtom, ADMMVariableEntry> varInfo : reasonerState.varMap.entrySet() ){
			GroundAtom var = varInfo.getKey();
			String debugStr="";
			debugStr+="Atom "+var.toString()+" with value "+
					formatDouble(var.getValue());
			double score = rnd.nextDouble();
			debugStr+=" assigned random score "+formatDouble(score);
			log.trace(debugStr);
			varScores.put(var, score );
		}
		return varScores;
	}
	
	
	private Map<GroundAtom,Double> scoreVariablesSimple(){
		HashMap<GroundAtom,Double> varScores= new HashMap<GroundAtom,Double>();
		for(Entry<GroundAtom, ADMMVariableEntry> varInfo : reasonerState.varMap.entrySet() ){
			GroundAtom var = varInfo.getKey();
			ADMMVariableEntry entry = varInfo.getValue();
			String debugStr="";
			double lagrangeScore = 0;
			double maxWeight = -1;
			debugStr+="Atom "+var.toString()+" with value "+
					formatDouble(var.getValue())+" has "+entry.objectiveTerms.size()+
					" optimization Lagrange multipliers: ";
			ArrayList<Double> vals = new ArrayList<Double>();
			for(ADMMReasoner.VariableLocation loc : entry.objectiveTerms){
				ADMMObjectiveTerm term = loc.term;
				int idx = loc.localIndex;
				lagrangeScore += Math.abs(term.y[idx]); //should be absolute value?
				vals.add(term.y[idx]);
				if(term instanceof WeightedObjectiveTerm &&
						((WeightedObjectiveTerm)term).getWeight() > maxWeight){
					maxWeight = ((WeightedObjectiveTerm)term).getWeight();
				}
			}
			debugStr+=doubleListToString(vals);
			if(maxWeight < 0 ){ maxWeight = 0; }
			debugStr+="and max weight "+formatDouble(maxWeight);
			double score = lagrangeCoeff * lagrangeScore + weightCoeff * maxWeight;
			debugStr+=" with final score "+formatDouble(score);
			log.trace(debugStr);
			varScores.put(var, score );
		}
		return varScores;
	}


	private Map<GroundAtom,Double> scoreVariablesUnsatisfied(String mode, boolean weighted){
		HashMap<GroundAtom,Double> varScores= new HashMap<GroundAtom,Double>();
		for(Entry<GroundAtom, ADMMVariableEntry> varInfo : reasonerState.varMap.entrySet() ){		
		//for(Entry<GroundAtom, List<GroundKernel>> varKernels : reasonerState.atomKernelMap.entrySet() ){
			GroundAtom var = varInfo.getKey();
			List<GroundKernel> kernels =  reasonerState.atomKernelMap.get(var);
			String debugStr="";
			double maxScore = -1;
			double totScore = 0;
			int termCnt = 0;
			debugStr+="Atom "+var.toString()+" with value "+
					formatDouble(var.getValue())+" has "+kernels.size()+
					" ground kernels: ";
			ArrayList<Double> vals = new ArrayList<Double>();
			ArrayList<String> wvals = new ArrayList<String>();
			for(GroundKernel kernel : kernels){
				termCnt++;
				if(kernel instanceof GroundCompatibilityKernel){
					GroundCompatibilityKernel gck = (GroundCompatibilityKernel)kernel;
					double incompatibility = gck.getIncompatibility();
					double score = weighted ? gck.getWeight().getWeight()*incompatibility : incompatibility;  
					if(maxScore < score){ maxScore = score; }
					totScore += score;
					vals.add(incompatibility);
					wvals.add(formatDouble(incompatibility)+"*"+formatDouble(gck.getWeight().getWeight()));
				} else if (kernel instanceof GroundConstraintKernel) {
					GroundConstraintKernel gck = (GroundConstraintKernel)kernel;
					double infeasibility = gck.getInfeasibility();
					double score = weighted ? this.constraint_weight*infeasibility : infeasibility;
					if(maxScore < score){ maxScore = score; }
					totScore += score;
					vals.add(infeasibility);
					wvals.add(formatDouble(infeasibility)+"*"+"C");
				}				
			}
			debugStr+= weighted ? stringListToString(wvals) : doubleListToString(vals);
			double score = maxScore;
			if(mode.equals("max")){
				score = maxScore;
			} else if(mode.equals("tot")){
				score = totScore;
			} else if(mode.equals("avg")){
				score = totScore/(double)termCnt;
			} else { log.info("No mode found for unsatisfication scoring, using max"); }
			debugStr+=" with final score "+formatDouble(score);
			log.trace(debugStr);
			varScores.put(var, score );
		}
		return varScores;
	}
	
	
	
	
	private Map<GroundAtom,Double> scoreVariablesRuleWeighted(String mode){
		return scoreVariablesLagrange(mode,true);
	}
	
	private Map<GroundAtom,Double> scoreVariablesLagrange(String mode, boolean weighted){
		HashMap<GroundAtom,Double> varScores= new HashMap<GroundAtom,Double>();
		for(Entry<GroundAtom, ADMMVariableEntry> varInfo : reasonerState.varMap.entrySet() ){
			GroundAtom var = varInfo.getKey();
			ADMMVariableEntry entry = varInfo.getValue();
			double maxScore = -1;
			double totScore=0;
			String debugStr="";
			int termCnt = 0;
			debugStr+="Atom "+var.toString()+" with value "+
					formatDouble(var.getValue())+" has "+entry.objectiveTerms.size()+
					" optimization Lagrange multipliers: ";
			ArrayList<Double> vals = new ArrayList<Double>();
			ArrayList<String> wvals = new ArrayList<String>();
			for(ADMMReasoner.VariableLocation loc : entry.objectiveTerms){
				termCnt++;
				ADMMObjectiveTerm term = loc.term;
				int idx = loc.localIndex;
				double lm = Math.abs(term.y[idx]);
				if(term instanceof WeightedObjectiveTerm){
					double score = weighted ? ((WeightedObjectiveTerm)term).getWeight()*lm : lm; 
					if(maxScore < score){ maxScore = score; }
					totScore += score;
					vals.add(term.y[idx]);
					wvals.add(formatDouble(term.y[idx])+"*"+formatDouble(((WeightedObjectiveTerm)term).getWeight()));
				} else {
					double score = weighted ? this.constraint_weight*lm : lm;
					if(maxScore < score){ maxScore = score; }
					totScore += score;
					vals.add(term.y[idx]);
					wvals.add(formatDouble(term.y[idx])+"*"+"C");
				}	
			}
			debugStr+= weighted ? stringListToString(wvals) : doubleListToString(vals);
			double score = maxScore;
			if(mode.equals("max")){
				score = maxScore;
			} else if(mode.equals("tot")){
				score = totScore;
			} else if(mode.equals("avg")){
				score = totScore/(double)termCnt;
			} else { log.info("No mode found for lagrange scoring, using max"); }
			debugStr+=" with final score "+formatDouble(score);
			log.trace(debugStr);
			varScores.put(var, score );
		}
		return varScores;
	}
	
	
	
	private Map<GroundAtom,Double> scoreVariablesFeaturePrinter(){
		HashMap<GroundAtom,Double> varScores= new HashMap<GroundAtom,Double>();
		int constraintWeight = 1000;
		System.out.println("==ACTIVATIONS==");
		for(Entry<GroundAtom, ADMMVariableEntry> varInfo : reasonerState.varMap.entrySet() ){
			GroundAtom var = varInfo.getKey();
			ADMMVariableEntry entry = varInfo.getValue();
			String debugStr="ACTIVATION_FEATURES: ";
			double maxLM = -1;
			double maxWeight = -1;
			double lagrangeTotal = 0;
			double weightTotal = 0;
			double weightedLMTotal = 0;
			int termCnt = 0;
			debugStr+=var.toString()+"\t"+
					"tv="+formatDouble(var.getValue())+"\t";
			ArrayList<Double> vals = new ArrayList<Double>();
			ArrayList<String> wvals = new ArrayList<String>();
			for(ADMMReasoner.VariableLocation loc : entry.objectiveTerms){
				termCnt++;
				ADMMObjectiveTerm term = loc.term;
				int idx = loc.localIndex;
				double lm = Math.abs(term.y[idx]);
				if(term instanceof WeightedObjectiveTerm){
					double wt = ((WeightedObjectiveTerm)term).getWeight(); 
					if(maxWeight < wt){ maxWeight = wt; }
					if(maxLM < lm){ maxLM = lm; }
					weightTotal += wt;
					lagrangeTotal += lm;
					weightedLMTotal += wt*lm;
					vals.add(term.y[idx]);
					wvals.add(formatDouble(term.y[idx])+"*"+formatDouble(((WeightedObjectiveTerm)term).getWeight()));
				} else {
					if(maxLM < lm){ maxLM = lm; }
					lagrangeTotal += lm;
					weightedLMTotal += constraintWeight*lm;
					vals.add(term.y[idx]);
					wvals.add(formatDouble(term.y[idx])+"*"+"C");
				}				
			}
			debugStr+="maxwt="+formatDouble(maxWeight)+"\t"+
					"avgwt="+formatDouble(weightTotal/(double)termCnt)+"\t"+
					"maxlm="+formatDouble(maxLM)+"\t"+
					"avglm="+formatDouble(lagrangeTotal/(double)termCnt)+"\t"+
					"avgwtlm="+formatDouble(weightedLMTotal/(double)termCnt)+"\t"+
					"cnt="+termCnt+"\t"+
					"lmwts=";
			debugStr+=stringListToString(wvals);
			//log.trace(debugStr);
			System.out.println(debugStr);
			varScores.put(var, (double)0 );
		}
		return varScores;
	}


	
	private Map<GroundAtom,Double> invertScores(Map<GroundAtom,Double> inMap){
		HashMap<GroundAtom,Double> invScores = new HashMap<GroundAtom,Double>();
		for(Entry<GroundAtom,Double> varScore : inMap.entrySet()){
			//note, for values below 0, we could simply do -1*val to get a consistent ordering that is positive
			if(varScore.getValue()<0){
				invScores.put(varScore.getKey(), -1*varScore.getValue());
				log.warn("Score less than zero - inverse may produce unexpected results!"); 
			} else {
				//note - adding 0.01 to smooth scores close to 0
				invScores.put(varScore.getKey(), 1/(varScore.getValue()+0.01));
			}
		}
		return invScores;
	}
	
	private Map<GroundAtom,Double> printScores(Map<GroundAtom,Double> varScores){
		if(print_scores){
			System.out.println("==SCORES==");
			for(GroundAtom ga : varScores.keySet()){						
				System.out.println(ga.toString()+"\t"+formatDouble(varScores.get(ga)));
			}
		}
		return varScores;
	}
	
	public Map<GroundAtom,Double> scoreVariables(){
		if(invert){
			log.info("Inverting score map");
			return printScores(invertScores(scoreVariablesInternal()));
		} else {
			return printScores(scoreVariablesInternal());
		}
	}
	private Map<GroundAtom,Double> scoreVariablesInternal(){
		log.info("Using "+score_method+" score_method to activate variables");
		if(score_method.equals("ruleweighted")){
			return scoreVariablesRuleWeighted("tot");
		} else if(score_method.equals("ruleweightedscaled")){
			return scoreVariablesRuleWeighted("avg");
		} else if(score_method.equals("ruleweightedmax")){
			return scoreVariablesRuleWeighted("max");
		} else if(score_method.equals("truthvalue")){
			return scoreVariablesTruthValue();
		} else if(score_method.equals("random")){
			return scoreVariablesRandom();
		} else if(score_method.equals("wtlagrangemax")){
			return scoreVariablesLagrange("max",true);
		} else if(score_method.equals("wtlagrangeavg")){
			return scoreVariablesLagrange("avg",true);
		} else if(score_method.equals("wtlagrangetot")){
			return scoreVariablesLagrange("tot",true);
		} else if(score_method.equals("lagrangemax")){
			return scoreVariablesLagrange("max",false);
		} else if(score_method.equals("lagrangeavg")){
			return scoreVariablesLagrange("avg",false);
		} else if(score_method.equals("lagrangetot")){
			return scoreVariablesLagrange("tot",false);
		} else if(score_method.equals("unsatisfiedmax")){
			return scoreVariablesUnsatisfied("max",false);
		} else if(score_method.equals("unsatisfiedavg")){
			return scoreVariablesUnsatisfied("avg",false);
		} else if(score_method.equals("unsatisfiedtot")){
			return scoreVariablesUnsatisfied("tot",false);
		} else if(score_method.equals("wtunsatisfiedmax")){
			return scoreVariablesUnsatisfied("max", true);
		} else if(score_method.equals("wtunsatisfiedavg")){
			return scoreVariablesUnsatisfied("avg", true);
		} else if(score_method.equals("wtunsatisfiedtot")){
			return scoreVariablesUnsatisfied("tot", true);
		} else if(score_method.equals("simple")) {
			return scoreVariablesSimple();
		} else if(score_method.equals("printfeatures")) {
			return scoreVariablesFeaturePrinter();
		} else {
			log.warn("No supported scoring method found! Using simple");
			return scoreVariablesSimple();
		}
	}
}
