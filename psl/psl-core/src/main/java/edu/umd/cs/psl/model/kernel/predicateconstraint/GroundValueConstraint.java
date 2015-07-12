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

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.builder.HashCodeBuilder;

import edu.umd.cs.psl.model.atom.Atom;
import edu.umd.cs.psl.model.atom.GroundAtom;
import edu.umd.cs.psl.model.kernel.BindingMode;
import edu.umd.cs.psl.model.kernel.ConstraintKernel;
import edu.umd.cs.psl.model.kernel.GroundConstraintKernel;
import edu.umd.cs.psl.model.kernel.GroundKernel;
import edu.umd.cs.psl.model.kernel.Kernel;
import edu.umd.cs.psl.reasoner.function.ConstantNumber;
import edu.umd.cs.psl.reasoner.function.ConstraintTerm;
import edu.umd.cs.psl.reasoner.function.FunctionComparator;
import edu.umd.cs.psl.reasoner.function.FunctionSum;
import edu.umd.cs.psl.reasoner.function.FunctionSummand;

public class GroundValueConstraint implements GroundConstraintKernel {
	private final ConstraintKernel kernel;
	private final Set<GroundAtom> atoms;
	private final GroundAtom atom;
	private final int hashcode;
	private final double value;
	
	public GroundValueConstraint(ConstraintKernel k, GroundAtom a, double value) {
		this.kernel = k;
		this.atoms = new HashSet<GroundAtom>();
		this.value = value;
		atoms.add(a);
		this.atom = a;
		this.hashcode = new HashCodeBuilder().append(kernel).append(atom).toHashCode();
		a.registerGroundKernel(this);
	}

	public ConstraintTerm getConstraintDefinition() {
		FunctionSum sum = new FunctionSum();
		sum.add(new FunctionSummand(1.0, atom.getVariable()));
		sum.add(new FunctionSummand(-1.0, new ConstantNumber(value)));
		return new ConstraintTerm(sum, FunctionComparator.Equality, 0.0);
	}
	
	@Override
	public boolean updateParameters() {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public double getInfeasibility() {
		return Math.abs(atom.getValue() - value);
	}
	
	@Override
	public int hashCode() {
		return hashcode;
	}


	@Override
	public ConstraintKernel getKernel() {
		return kernel;
	}

	@Override
	public Set<GroundAtom> getAtoms() {
		return atoms;
	}

	@Override
	public BindingMode getBinding(Atom atom) {
		if(this.atom.equals(atom)){
			return BindingMode.WeakRV;
		}
		return BindingMode.NoBinding;
	}
	
	@Override
	public boolean equals(Object oth) {
		if (oth == this)
			return true;
		if (oth == null || !(getClass().isInstance(oth)))
			return false;
		GroundValueConstraint con = (GroundValueConstraint) oth;
		return (atom.equals(con.atom) && value == con.value); 
	}

	@Override
	public String toString() {
		return "{Value} on " + atom.toString() + " = " + Double.toString(value);
	}


}
