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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.mathnbits.util.KeyedRetrievalSet;
import edu.umd.cs.psl.model.atom.GroundAtom;
import edu.umd.cs.psl.model.atom.RandomVariableAtom;
import edu.umd.cs.psl.model.kernel.GroundConstraintKernel;
import edu.umd.cs.psl.model.kernel.GroundKernel;
import edu.umd.cs.psl.model.kernel.Kernel;
import edu.umd.cs.psl.reasoner.admm.ADMMReasoner.VariableLocation;
import edu.umd.cs.psl.reasoner.function.AtomFunctionVariable;
import edu.umd.cs.psl.reasoner.function.MutableAtomFunctionVariable;
import edu.umd.cs.psl.util.collection.HashList;

public class ADMMReasonerState {

	Map<GroundAtom,ADMMVariableEntry> varMap;
	Map<GroundAtom,List<GroundKernel>> atomKernelMap;
	Map<ADMMObjectiveTerm,GroundKernel> termMap;
	Map<GroundAtom,Set<GroundAtom>> constrainedPairs;
	
	public ADMMReasonerState(ADMMReasoner reasoner) {
		varMap = new HashMap<GroundAtom,ADMMVariableEntry>(reasoner.variables.size());
		termMap = new HashMap<ADMMObjectiveTerm,GroundKernel>(reasoner.terms.size());
		atomKernelMap = new HashMap<GroundAtom,List<GroundKernel>>(reasoner.variables.size());
		constrainedPairs = new HashMap<GroundAtom,Set<GroundAtom>>(reasoner.variables.size());
		for(int i = 0; i < reasoner.variables.size(); i++){
			ADMMVariableEntry varEntry = new ADMMVariableEntry();
			varEntry.consensusEstimate = reasoner.z.get(i);
			varEntry.upperBound = reasoner.ub.get(i);
			varEntry.lowerBound = reasoner.lb.get(i);
			varEntry.objectiveTerms = new ArrayList<VariableLocation>(reasoner.varLocations.get(i));
			varMap.put(reasoner.variables.get(i).getAtom(), varEntry);
		}
		for(int i = 0; i < reasoner.terms.size(); i++){
			ADMMObjectiveTerm term = reasoner.terms.get(i);
			GroundKernel gk = reasoner.orderedGroundKernels.get(i);
			termMap.put(term, gk);
			if(gk instanceof GroundConstraintKernel){
				for(int zi = 0; zi < term.zIndices.length; zi++){
					GroundAtom var = reasoner.variables.get(term.zIndices[zi]).getAtom();
					if(!constrainedPairs.containsKey(var)){
						constrainedPairs.put(var, new HashSet<GroundAtom>());
					}
					for(int zj = 0; zj < term.zIndices.length; zj++){
						if(zj != zi){
							constrainedPairs.get(var).add( reasoner.variables.get(term.zIndices[zj]).getAtom());
						}
					}//inner loop - value added to set
				} //outer loop - key
			}//add constrained pairs
			
			for(GroundAtom g : gk.getAtoms()){
				//TODO: should we convert g to a QueryAtom?
				if(!atomKernelMap.containsKey(g)){
					atomKernelMap.put(g,new ArrayList<GroundKernel>());
				}
				atomKernelMap.get(g).add(gk);
			}//atoms
		}//terms
	}

	public Set<GroundKernel> getAtomKernels(GroundAtom atom){
		if(!atomKernelMap.containsKey(atom)){ return new HashSet<GroundKernel>(); }
		HashSet<GroundKernel> retSet = new HashSet<GroundKernel>(atomKernelMap.get(atom));
		return retSet;
	}
	
	public Map<GroundAtom, ADMMVariableEntry> getVariableStates(){
		return varMap;
	}
	
	public class ADMMVariableEntry {
		public double consensusEstimate;
		public double upperBound;
		public double lowerBound;
		List<VariableLocation> objectiveTerms;
	}

	public Set<GroundAtom> constrainedPairs(GroundAtom var) {
		if(constrainedPairs.containsKey(var)){
			return constrainedPairs.get(var);
		} else {
			return new HashSet<GroundAtom>();
		}
	}

}
