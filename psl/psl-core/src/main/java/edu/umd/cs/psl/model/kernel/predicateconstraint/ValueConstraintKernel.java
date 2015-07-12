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
package edu.umd.cs.psl.model.kernel.predicateconstraint;

import edu.umd.cs.psl.application.groundkernelstore.GroundKernelStore;
import edu.umd.cs.psl.model.atom.AtomEvent;
import edu.umd.cs.psl.model.atom.AtomEventFramework;
import edu.umd.cs.psl.model.atom.AtomManager;
import edu.umd.cs.psl.model.kernel.AbstractKernel;
import edu.umd.cs.psl.model.kernel.ConstraintKernel;

public class ValueConstraintKernel extends AbstractKernel implements
		ConstraintKernel {

	public ValueConstraintKernel() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void groundAll(AtomManager atomManager, GroundKernelStore gks) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void notifyAtomEvent(AtomEvent event, GroundKernelStore gks) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void registerForAtomEvents(AtomEventFramework eventFramework) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void unregisterForAtomEvents(AtomEventFramework eventFramework) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public String toString() {
		return "{constraint} Value (generic)";
	}

}
