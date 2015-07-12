/*
 * This file is part of the PSL software.
 * Copyright 2011-2013 University of Maryland
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

import de.mathnbits.util.KeyedRetrievalSet;
import edu.umd.cs.psl.config.ConfigBundle;
import edu.umd.cs.psl.config.ConfigManager;
import edu.umd.cs.psl.model.atom.GroundAtom;
import edu.umd.cs.psl.model.kernel.GroundCompatibilityKernel;
import edu.umd.cs.psl.model.kernel.GroundConstraintKernel;
import edu.umd.cs.psl.model.kernel.GroundKernel;
import edu.umd.cs.psl.model.kernel.Kernel;
import edu.umd.cs.psl.reasoner.Reasoner;
import edu.umd.cs.psl.reasoner.admm.ADMMReasoner.Hyperplane;
import edu.umd.cs.psl.reasoner.admm.ADMMReasoner.VariableLocation;
import edu.umd.cs.psl.reasoner.function.AtomFunctionVariable;
import edu.umd.cs.psl.reasoner.function.ConstantNumber;
import edu.umd.cs.psl.reasoner.function.ConstraintTerm;
import edu.umd.cs.psl.reasoner.function.FunctionSingleton;
import edu.umd.cs.psl.reasoner.function.FunctionSum;
import edu.umd.cs.psl.reasoner.function.FunctionSummand;
import edu.umd.cs.psl.reasoner.function.FunctionTerm;
import edu.umd.cs.psl.reasoner.function.MaxFunction;
import edu.umd.cs.psl.reasoner.function.PowerOfTwo;
import edu.umd.cs.psl.util.collection.HashList;
import edu.umd.cs.psl.util.concurrent.ThreadPool;

/**
 * Uses an ADMM optimization method to optimize its GroundKernels.
 * 
 * @author Stephen Bach <bach@cs.umd.edu>
 * @author Eric Norris
 * @author Jay Pujara <jay@cs.umd.edu>
 */
public class StreamingADMMReasoner extends ADMMReasoner {
	
	private static final Logger log = LoggerFactory.getLogger(StreamingADMMReasoner.class);
	
	/**
	 * Prefix of property keys used by this class.
	 * 
	 * @see ConfigManager
	 */
	public static final String CONFIG_PREFIX = "streamingadmmreasoner";

	public StreamingADMMReasoner(ConfigBundle config) {
		super(config);
	}
	
	public Set<GroundAtom> getReasonerAtoms(){
		Set<GroundAtom> atoms = new HashSet<GroundAtom>();
		for(AtomFunctionVariable var : variables){
			atoms.add(var.getAtom());
		}
		return atoms;
	}
	
	public void buildGroundModel(){
		super.buildGroundModel();
	}
	
	public void setVariableBounds(AtomFunctionVariable a, double lb, double ub){
		if(variables.contains(a)){
			int i = variables.indexOf(a);
			super.lb.set(i, lb);
			super.ub.set(i, ub);
		}
	}
	
	
}
