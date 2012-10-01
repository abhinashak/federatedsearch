/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.row;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.thirdparty.guava.common.collect.HashMultimap;
import org.apache.hadoop.thirdparty.guava.common.collect.Multimap;

public class RowJoiner {

	private RowJoiner() {
		
	}
	
	public static void and (List<IRowId<?,?>> input1, List<IRowId<?,?>> input2) {

		if ( null == input1 || null == input2) {
			return;
		}
		
		if ( 0 == input1.size()|| 0 == input2.size()) {
			input1.clear();
			input2.clear();
			return;
		}
		
		List<IRowId<?,?>> set1 = null;
		List<IRowId<?,?>> set2 = null;
		if ( input1.size() > input2.size()) {
			set1 = input2;
			set2 = input1;
		} else {
			set1 = input1;
			set2 = input2;
		}
		
		/**
		 * Step 1 - Create a Set of Buckets.
		 */
		Set<String> set1Buckets = new HashSet<String>();
		
		for (IRowId<?,?> id1 : set1) {
			set1Buckets.add(id1.getPartition().toString());
		}

		
		/**
		 * Step 2 - Find intersected buckets
		 */
		Set<String> intersectedBuckets = new HashSet<String>();
		for (IRowId<?,?> id2 : set2) {
			if ( set1Buckets.contains(id2.getPartition().toString())) 
				intersectedBuckets.add(id2.getPartition().toString());
		}
		set1Buckets.clear();
		
		/**
		 * Step 3 - Cleanse unnecessary elements set 1
		 */
		keepMatchings(set1, intersectedBuckets);
		
		/**
		 * Step 4 - Cleanse unnecessary elements set 2
		 */
		keepMatchings(set2, intersectedBuckets);
		
		/**
		 * Step 5 - Extract Set 1 from Set 2
		 */
		subtract(set1, set2);

		/**
		 * Step 6 - Extract Set 2 from Set 1
		 */
		subtract(set2, set1);
	}
	
	public static void not (List<IRowId<?,?>> input1, List<IRowId<?,?>> input2) {

		Multimap<String,String> mmm = HashMultimap.create();
		for (IRowId<?,?> aRecord : input2) {
			mmm.put(aRecord.getPartition().toString(), aRecord.getDocId().toString());
		}
		
		/**
		 * Discard Now
		 */
		List<IRowId<?,?>> notInRecords = new ArrayList<IRowId<?,?>>();
		for( IRowId<?,?> record : input1) {
			if ( mmm.get(record.getPartition().toString()).contains(record.getDocId().toString()) ) continue;
			notInRecords.add(record);
		}
		input1.clear();
		input1.addAll(notInRecords);
		notInRecords.clear();
		mmm.clear();

	}

	
	public static void or (List<IRowId<?,?>> input1, List<IRowId<?,?>> input2) {
		
		Multimap<String,String> mmm = HashMultimap.create();
		
		if ( null != input1) {
			for (IRowId<?,?> recordId : input1) {
				mmm.put(recordId.getPartition().toString(), recordId.getDocId().toString());
			}
		}

		List<IRowId<?,?>> delta = new ArrayList<IRowId<?,?>>();
		if ( null != input2) {
			for (IRowId<?,?> recordId : input2) {
				if ( ! mmm.containsKey(recordId.getPartition().toString()) ) {
					delta.add(recordId);
					continue;
				}
				
				if ( mmm.get(recordId.getPartition().toString()).contains(recordId.getDocId().toString())) continue;
				delta.add(recordId);
			}
		} else {
			System.out.println("input1 is null");
		}
		mmm.clear();
		input1.addAll(delta);
		delta.clear();
	}	

	private static void subtract(List<IRowId<?,?>> set1, List<IRowId<?,?>> set2) {
		/**
		 * Set 1 is smaller than Set2
		 */
		Multimap<String,String> mmm = HashMultimap.create();
		for (IRowId<?,?> aRecord : set1) {
			mmm.put(aRecord.getPartition().toString(), aRecord.getDocId().toString());
		}
		
		/**
		 * Merge Now
		 */
		List<IRowId<?,?>> matchingRecords = new ArrayList<IRowId<?,?>>();
		for( IRowId<?,?> record : set2) {
			if ( ! mmm.get(record.getPartition().toString()).contains(record.getDocId()) ) continue;
				matchingRecords.add(record);
		}
		set2.clear();
		set2.addAll(matchingRecords);
		matchingRecords.clear();
		mmm.clear();
	}

	
	private static void keepMatchings(List<IRowId<?,?>> records, Set<String> intersectedBuckets) {
		int setT = records.size();
		
		int removed = 0;
		IRowId<?,?> aRecord = null;
		for ( int i=0; i<setT; i++) {
			aRecord = records.get(i);
			if (intersectedBuckets.contains(aRecord.getPartition().toString()) ) continue;
			removed++;
		}
		
		if ( removed > 5000  ) {
			List<IRowId<?,?>> cleansedLists = new ArrayList<IRowId<?,?>>(records.size()  - removed);
			for ( int i=0; i<setT; i++) {
				aRecord = records.get(i);
				if ( ! intersectedBuckets.contains(aRecord.getPartition().toString()) ) continue;
				cleansedLists.add(aRecord);
			}
			records.clear();
			records.addAll(cleansedLists);
			cleansedLists.clear();
		} else {
			for ( int i=0; i<setT; i++) {
				aRecord = records.get(i);
				if (intersectedBuckets.contains(aRecord.getPartition().toString()) ) continue;
				records.remove(i);
				i--;
				setT--;
			}				
		}
	}
	

	public List<IRowId<Long, Integer>> testDataSet(int startBucketIndex, int endBucketIndex, short startDocIndex, short endDocIndex) {
		
		List<IRowId<Long, Integer>> set = new ArrayList<IRowId<Long, Integer>>(); //Sorted by weight 
		for ( long i=startBucketIndex; i<endBucketIndex; i++) {
			for ( int j=startDocIndex; j<endDocIndex; j++) {
				IRowId<Long, Integer> aRow = new PartitionedRow<Long, Integer>(i,j); 
				set.add(aRow);
			}
		}
		return set;
	}
}
