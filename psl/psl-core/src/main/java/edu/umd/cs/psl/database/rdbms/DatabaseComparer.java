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
package edu.umd.cs.psl.database.rdbms;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.healthmarketscience.sqlbuilder.BinaryCondition;
import com.healthmarketscience.sqlbuilder.ComboCondition;
import com.healthmarketscience.sqlbuilder.CustomSql;
import com.healthmarketscience.sqlbuilder.InCondition;
import com.healthmarketscience.sqlbuilder.SelectQuery;
import com.healthmarketscience.sqlbuilder.SelectQuery.JoinType;
import com.healthmarketscience.sqlbuilder.Subquery;
import com.healthmarketscience.sqlbuilder.UnaryCondition;

import edu.umd.cs.psl.database.Database;
import edu.umd.cs.psl.model.argument.ArgumentType;
import edu.umd.cs.psl.model.argument.DoubleAttribute;
import edu.umd.cs.psl.model.argument.GroundTerm;
import edu.umd.cs.psl.model.argument.IntegerAttribute;
import edu.umd.cs.psl.model.argument.StringAttribute;
import edu.umd.cs.psl.model.atom.QueryAtom;
import edu.umd.cs.psl.model.predicate.Predicate;

public class DatabaseComparer {
    

	RDBMSDataStore ds = null;
	RDBMSDatabase db1 = null;
	RDBMSDatabase db2 = null;

	public DatabaseComparer(RDBMSDataStore ds,
			RDBMSDatabase db1, RDBMSDatabase db2) {
		this.ds = ds;
		this.db1 = db1;
		this.db2 = db2;
	}

	private ArrayList<Integer> makeDatabasePartitionsList(RDBMSDatabase db){
		ArrayList<Integer> partitions = new ArrayList<Integer>(db.readPartitions.length+1);
		for (int i = 0; i < db.readPartitions.length; i++)
			partitions.add(db.readPartitions[i].getID());
		partitions.add(db.writePartition.getID());
		return partitions;
	}

	protected GroundTerm[] parseArgumentsFromResultSet(Predicate p, ResultSet rs) throws SQLException{
		GroundTerm [] args = new GroundTerm[p.getArity()];
		for (int pos = 0; pos < p.getArity(); pos++) {
			ArgumentType type = p.getArgumentType(pos);
			switch (type) {
			case Double:
				args[pos] = new DoubleAttribute(rs.getDouble(pos+1));
				break;
			case Integer:
				args[pos] = new IntegerAttribute(rs.getInt(pos+1));
				break;
			case String:
				args[pos] = new StringAttribute(rs.getString(pos+1));
				break;
			case UniqueID:
				args[pos] = db1.getUniqueID(rs.getObject(pos+1));
				break;
			default:
				throw new IllegalArgumentException("Unknown argument type: " + type);
			}
		}
		return args;
	}


	private ArrayList<QueryAtom> makeAtomListFromQuery(Database db, Predicate p, String q){
		ArrayList<QueryAtom> atoms = new ArrayList<QueryAtom>();
		Connection c = ds.getConnection();
		try {
			Statement s = c.createStatement();
			try{
				ResultSet rs = s.executeQuery(q);
				try {
					while (rs.next()) {
						atoms.add(new QueryAtom(p, parseArgumentsFromResultSet(p,rs)));
					}
				} finally {
					rs.close();
				}
			} finally {
				s.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException("Error executing database query.", e);
		}
		//Queries.convertArguments(db1, p, )
		return atoms;
	}

	public ArrayList<QueryAtom> queryUpdatedAtoms(Predicate p){

		SelectQuery q = new SelectQuery();

		/*SELECT * 
		 * FROM EVIDENCE_PREDICATE as E1, 
		 * EVIDENCE_PREDICATE as E2 
		 * WHERE E1.UNIQUEID_0 = E2.UNIQUEID_0 
		 * AND E1.UNIQUEID_1 = E2.UNIQUEID_1 
		 * AND E1.TRUTH != E2.TRUTH 
		 * AND E1.PARTITION in (6,7,8,9) 
		 * AND E2.PARTITION IN (6,7,11,12)
		 */

		RDBMSPredicateHandle ph1 = db1.getHandle(p);
		RDBMSPredicateHandle ph2 = db1.getHandle(p);

		String t1Alias = ph1.tableName() + " T1";
		String t2Alias = ph2.tableName() + " T2";

		q.addCustomFromTable(t1Alias);
		q.addCustomFromTable(t2Alias);

		for (int i = 0; i < ph1.argumentColumns().length; i++) {
			q.addCondition(BinaryCondition.equalTo(
					new CustomSql("T1."
							+ ph1.argumentColumns()[i]),
					new CustomSql("T2."
							+ ph2.argumentColumns()[i])
					));
			//q.addAliasedColumn(new CustomSql("T1."+ph1.argumentColumns()[i]),	);
			q.addCustomColumns(new CustomSql("T1."+ph1.argumentColumns()[i]));
		}
		q.addCondition(BinaryCondition.notEqualTo(
				new CustomSql("T1."+ph1.valueColumn()), 
				new CustomSql("T2."+ph2.valueColumn())
				));



		q.addCondition(new InCondition(
				new CustomSql("T1."+ph1.partitionColumn()),
				makeDatabasePartitionsList(db1)));
		q.addCondition(new InCondition(
				new CustomSql("T2."+ph2.partitionColumn()),
				makeDatabasePartitionsList(db2)));


		return makeAtomListFromQuery(db1,p,q.toString());
	}
	
	private SelectQuery makeDatabasePredicateQuery(RDBMSDatabase db, Predicate p){
		SelectQuery sq = new SelectQuery();
		RDBMSPredicateHandle ph = db.getHandle(p);
		sq.addCustomFromTable(new CustomSql(ph.tableName()) );
		for (int i = 0; i < ph.argumentColumns().length; i++) {
			sq.addCustomColumns(new CustomSql(ph.argumentColumns()[i]));
		}
		sq.addCustomColumns(new CustomSql(ph.valueColumn()));
		sq.addCondition(new InCondition(
				new CustomSql(ph.partitionColumn()),
				makeDatabasePartitionsList(db)));
		return sq;
	}
	
	public ArrayList<QueryAtom> queryAddedAtoms(Predicate p){
		SelectQuery q = new SelectQuery();
		Subquery sq1 = new Subquery(makeDatabasePredicateQuery(db1,p));
		Subquery sq2 = new Subquery(makeDatabasePredicateQuery(db2,p));
		
		/* SELECT E1.UNIQUEID_0, E1.UNIQUEID_1 FROM  
		 * 	(SELECT UNIQUEID_0, UNIQUEID_1, TRUTH 
		 * 	FROM EVIDENCE_PREDICATE 
		 * 	WHERE PARTITION IN (6,7,8,9)) as E1 
		 * LEFT OUTER JOIN 
		 * 	(SELECT UNIQUEID_0, UNIQUEID_1, TRUTH 
		 * 	FROM EVIDENCE_PREDICATE 
		 * 	WHERE PARTITION IN (6,7,41,42)) as E2 
		 * ON E1.UNIQUEID_0 = E2.UNIQUEID_0  
		 * AND E1.UNIQUEID_1 = E2.UNIQUEID_1 
		 * WHERE E2.TRUTH IS NULL
		 */
		
		String t1Alias = sq1.toString() + " T1";
		String t2Alias = sq2.toString() + " T2";
		
		RDBMSPredicateHandle ph = db1.getHandle(p);
		ComboCondition cond = new ComboCondition(ComboCondition.Op.AND);
		
		for (int i = 0; i < ph.argumentColumns().length; i++) {
			cond.addCondition(BinaryCondition.equalTo(
					new CustomSql("T1."
							+ ph.argumentColumns()[i]),
					new CustomSql("T2."
							+ ph.argumentColumns()[i])
					));
			//q.addAliasedColumn(new CustomSql("T1."+ph1.argumentColumns()[i]),	);
			q.addCustomColumns(new CustomSql("T1."+ph.argumentColumns()[i]));
		}
		q.addCustomJoin(JoinType.LEFT_OUTER, new CustomSql(t1Alias), 
				new CustomSql(t2Alias), cond);
		q.addCondition(UnaryCondition.isNull(
				new CustomSql("T2."+ph.valueColumn())
				));

		return makeAtomListFromQuery(db1,p,q.toString());
	
	}
	public ArrayList<QueryAtom> queryRemovedAtoms(Predicate p){
		SelectQuery q = new SelectQuery();
		Subquery sq1 = new Subquery(makeDatabasePredicateQuery(db1,p));
		Subquery sq2 = new Subquery(makeDatabasePredicateQuery(db2,p));
		
		/* SELECT E2.UNIQUEID_0, E2.UNIQUEID_1 FROM  
		 * 	(SELECT UNIQUEID_0, UNIQUEID_1, TRUTH 
		 * 	FROM EVIDENCE_PREDICATE 
		 * 	WHERE PARTITION IN (6,7,8,9)) as E1 
		 * RIGHT OUTER JOIN 
		 * 	(SELECT UNIQUEID_0, UNIQUEID_1, TRUTH 
		 * 	FROM EVIDENCE_PREDICATE 
		 * 	WHERE PARTITION IN (6,7,41,42)) as E2 
		 * ON E1.UNIQUEID_0 = E2.UNIQUEID_0  
		 * AND E1.UNIQUEID_1 = E2.UNIQUEID_1 
		 * WHERE E1.TRUTH IS NULL
		 */
		
		String t1Alias = sq1.toString() + " T1";
		String t2Alias = sq2.toString() + " T2";
		
		RDBMSPredicateHandle ph = db1.getHandle(p);
		ComboCondition cond = new ComboCondition(ComboCondition.Op.AND);
		
		for (int i = 0; i < ph.argumentColumns().length; i++) {
			cond.addCondition(BinaryCondition.equalTo(
					new CustomSql("T1."
							+ ph.argumentColumns()[i]),
					new CustomSql("T2."
							+ ph.argumentColumns()[i])
					));
			//q.addAliasedColumn(new CustomSql("T1."+ph1.argumentColumns()[i]),	);
			q.addCustomColumns(new CustomSql("T2."+ph.argumentColumns()[i]));
		}
		q.addCustomJoin(JoinType.RIGHT_OUTER, new CustomSql(t1Alias), 
				new CustomSql(t2Alias), cond);
		q.addCondition(UnaryCondition.isNull(
				new CustomSql("T1."+ph.valueColumn())
				));

		return makeAtomListFromQuery(db1,p,q.toString());

	}
}
