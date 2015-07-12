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
import java.util.*;

/**
 * Sort maps based on StackOverflow advice,
 * http://stackoverflow.com/questions/109383/how-to-sort-a-mapkey-value-on-the-values-in-java
 * @author jay
 *
 */
public class MapSorter {
	public MapSorter() {}

	public static <K, V extends Comparable<? super V>> Map<K,V> subselectMapSorted(Set<K> keys, Map<K,V> inMap){
		Map<K, V> result = new LinkedHashMap<K, V>();
		for(K k : keys){
			if(inMap.containsKey(k)){
				result.put(k, inMap.get(k));
			}
		}
		return sortByValueDescending(result);
	}

	public static <K, V extends Comparable<? super V>> Map<K, V> 
	sortByValue( Map<K, V> map )
	{
		List<Map.Entry<K, V>> list =
				new LinkedList<Map.Entry<K, V>>( map.entrySet() );
		Collections.sort( list, new Comparator<Map.Entry<K, V>>()
				{
			public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
			{
				return (o1.getValue()).compareTo( o2.getValue() );
			}
				} );

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list)
		{
			result.put( entry.getKey(), entry.getValue() );
		}
		return result;
	}

	public static <K, V extends Comparable<? super V>> Map<K, V> 
	sortByValueDescending( Map<K, V> map )
	{
		List<Map.Entry<K, V>> list =
				new LinkedList<Map.Entry<K, V>>( map.entrySet() );
		Collections.sort( list, new Comparator<Map.Entry<K, V>>()
				{
			public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
			{
				int cmp = (o2.getValue()).compareTo( o1.getValue() );
				if(cmp == 0){
					cmp = (o2.getKey().toString()).compareTo( o1.getKey().toString() );					
				}
				return cmp;
			}
				} );

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list)
		{
			result.put( entry.getKey(), entry.getValue() );
		}
		return result;
	}
}
