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
package edu.umd.cs.psl.evaluation.result.memory;

import edu.umd.cs.psl.reasoner.admm.ADMMReasonerState;

public class ReasonerFullInferenceResult extends MemoryFullInferenceResult {

	ADMMReasonerState state;
	public ReasonerFullInferenceResult(double incomp, double infNorm,
			int noAtoms, int numGevidence, ADMMReasonerState state) {
		super(incomp, infNorm, noAtoms, numGevidence);
		this.state = state;
	}
	
	public ADMMReasonerState getADMMReasonerState(){
		return state;
	}

}
