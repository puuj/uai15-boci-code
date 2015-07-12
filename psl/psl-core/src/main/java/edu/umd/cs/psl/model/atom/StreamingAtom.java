package edu.umd.cs.psl.model.atom;

import edu.umd.cs.psl.database.Stream;
import edu.umd.cs.psl.model.argument.ArgumentType;
import edu.umd.cs.psl.model.argument.Term;
import edu.umd.cs.psl.model.argument.Variable;
import edu.umd.cs.psl.model.argument.VariableTypeMap;
import edu.umd.cs.psl.model.predicate.Predicate;

public class StreamingAtom extends Atom {
	private final Stream stream;
	public StreamingAtom(Predicate p, Term[] args, Stream s) {
		super(p, args);
		this.stream = s;
	}
	
	public Stream getStream(){
		return this.stream;
	}

	@Override
	public VariableTypeMap collectVariables(VariableTypeMap varMap) {
		for (int i=0;i<arguments.length;i++) {
			if (arguments[i] instanceof Variable) {
				ArgumentType t = predicate.getArgumentType(i);
				varMap.addVariable((Variable)arguments[i], t);
			}
		}
		return varMap;
	}

}
