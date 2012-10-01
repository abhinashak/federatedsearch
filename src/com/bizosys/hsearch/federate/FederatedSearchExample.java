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

package com.bizosys.hsearch.federate;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.thirdparty.guava.common.collect.HashMultimap;
import org.apache.hadoop.thirdparty.guava.common.collect.Multimap;

import com.bizosys.hsearch.query.HQuery.HTerm;
import com.bizosys.hsearch.query.HResult;
import com.bizosys.hsearch.row.IRowId;
import com.bizosys.hsearch.row.PartitionedRow;

public class FederatedSearchExample extends FederatedCombiner {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		FederatedSearchExample example = new FederatedSearchExample();
		List<IRowId<?,?>> finalResult = example.combine("(fedQ1 NOT fedQ3)");
		
		Multimap<String,String> mmm = HashMultimap.create();
		for (IRowId<?,?> aRecord : finalResult) {
			mmm.put(aRecord.getPartition().toString(), aRecord.getDocId().toString());
		}
		System.out.println(mmm.toString());
		
	}
	

	@Override
	public IFederatedSource buildSource(HTerm aTerm) {
		return new FederatedSourceTest();
	}	
	
	public static class FederatedSourceTest implements IFederatedSource {

		HTerm term = null;
		
		@Override
		public void setTerm(HTerm term) {
			this.term = term;
		}
		
		@Override
		public HTerm getTerm() {
			return this.term;
		}
		
		@Override
		public void execute() {

			int howMany = new Random().nextInt(30);
			int startRow = new Random().nextInt(100);
			int endRow = new Random().nextInt(100);
			if ( 0 <= howMany) howMany = 2;
			
			if ( endRow < startRow ) endRow = startRow + 1;
			System.out.println("howMany:" + howMany + ", startR:" + startRow + " , endRow:" + endRow);
			List<IRowId<?, ?>> rowIds = new ArrayList<IRowId<?, ?>>();
			for ( long i=0; i<howMany; i++) {
				for ( int j=startRow; j<endRow; j++) {
					System.out.println(i + ":" + j);
					rowIds.add(new PartitionedRow<Long, Integer>(i,j));
				}
			}
			HResult result = new HResult();
			result.setRowIds(rowIds);
			
			term.setResult(result);
		}

	}

}
