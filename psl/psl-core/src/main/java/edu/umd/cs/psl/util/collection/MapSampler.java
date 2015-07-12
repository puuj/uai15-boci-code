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
package edu.umd.cs.psl.util.collection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.psl.reasoner.admm.ADMMStateActivator;

public class MapSampler {
	private static final Logger log = LoggerFactory.getLogger(MapSampler.class);

	public MapSampler() {
	}
	
	private static <K, V extends Number > K selectKey(List<K> order, Map<K,V>map, double selector){
		double curr=0;
		for(int i = 0; i < order.size(); i++){
			curr += Math.abs(map.get(order.get(i)).doubleValue());
			if(curr >= selector){
				return order.get(i);
			}
		}
		return order.get(order.size()-1);
	}
	
	public static <K> K sampleRepresentative(Map<K,Number>map){
		double curr=0;
		Random rnd = new Random();
		double selector = rnd.nextDouble()*computeTotal(map);
		for(K k : map.keySet()){
			curr += Math.abs(map.get(k).doubleValue());
			if(curr >= selector){
				return k;
			}
		}
		return null;
	}
	public static <K> K sampleRepresentative(Map<K,Number>map, double selector){
		double curr=0;
		for(K k : map.keySet()){
			curr += Math.abs(map.get(k).doubleValue());
			if(curr >= selector){
				return k;
			}
		}
		return null;
	}

	private static <K,V extends Number> double computeTotal(Map<K,V>map){
		double total = 0.0;
		for(Number n : map.values()){
			if(n.doubleValue() < 0){
					log.warn("Map contains negative values. Sampling will fail.");
				}
				total += Math.abs(n.doubleValue());
		}
		return total;
	}
	
	
	private static <K,V extends Number> double computeTotal(Map<K,V>map, List<K> keys){
		double total = 0.0;
		for(K key : keys){
			Number n = map.get(key);
			if(n.doubleValue() < 0){
					log.warn("Map contains negative values. Sampling will fail.");
				}
				total += Math.abs(n.doubleValue());
		}
		return total;
	}
	
	public static <K, V extends Number> Set<K> sampleRepresentativeSet( Map<K, V> map , int numToSelect){
		Random rnd = new Random();
		Set<K> retSet = new HashSet<K>();
		List<K> orderedKeys = new LinkedList<K>();
		orderedKeys.addAll(map.keySet());
		int toSelect = Math.min(numToSelect, map.size());
		int sampleLimit = 10*toSelect;
		double total = computeTotal(map,orderedKeys);
		int selected = 0;
		int rounds = 0;
		while(selected < toSelect && rounds < sampleLimit){
			rounds++;
			K select = selectKey(orderedKeys, map, total*rnd.nextDouble());
			if(!retSet.contains(select)) {
				retSet.add(select);
				orderedKeys.remove(select);
				selected++;
				total = computeTotal(map,orderedKeys);
			}
		}
		if(rounds >= sampleLimit){
			//Introduced if there are many zero or near-zero values.
			log.error("Hit sampling limit, returning "+selected+" instead of "+toSelect);
		}
		return retSet;
	}
}

