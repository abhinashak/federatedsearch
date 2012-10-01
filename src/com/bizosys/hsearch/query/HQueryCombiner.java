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

package com.bizosys.hsearch.query;

import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.query.HQuery.HTerm;
import com.bizosys.hsearch.row.IRowId;
import com.bizosys.hsearch.row.RowJoiner;

public class HQueryCombiner {

	public static  List<IRowId<?,?>> combine(HQuery query, List<IRowId<?,?>> finalResult) throws Exception  {
		
		boolean isFirst = true;
		for (HQuery subQuery : query.subQueries) {
			List<IRowId<?,?>> subQResult = new ArrayList<IRowId<?,?>>();
			combine(subQuery, subQResult);
			if ( subQuery.isShould) {
				RowJoiner.or(finalResult, subQResult);
			} else if ( subQuery.isMust) {
				RowJoiner.and(finalResult, subQResult);
			} else {
				RowJoiner.not(finalResult, subQResult);
			}
			subQResult.clear();
		}
		
		//Find must terms and add them
		for (HTerm term : query.terms) {
			if ( term.isShould ) continue;
			if ( ! term.isMust ) continue;

			if ( isFirst ) {
				finalResult.addAll(term.getResult().getRowIds());
				System.out.println("First Must :" + term.text + ":" + finalResult.size());
				isFirst = false;
			} else {
				RowJoiner.and(finalResult, term.getResult().getRowIds());
				System.out.println("Subsequnt Must :" + term.text + ":" + finalResult.size());
			}
		}
		
		//OR Terms
		for (HTerm term : query.terms) {
			if ( ! term.isShould ) continue;

			if ( isFirst ) {
				finalResult.addAll(term.getResult().getRowIds());
				isFirst = false;
				System.out.println("First OR :" + term.text + ":" + finalResult.size());
			} else {
				RowJoiner.or(finalResult, term.getResult().getRowIds());
				System.out.println("Subsequent OR :" + term.text + ":" + finalResult.size());
			}
		}
		
		for (HTerm term : query.terms) {
			if ( term.isShould ) continue;
			if ( term.isMust) continue;

			if ( isFirst ) {
				throw new RuntimeException("Only must not query not allowed");
			} else {
				RowJoiner.not(finalResult, term.getResult().getRowIds());
				System.out.println("Not :" + term.text + ":" + finalResult.size());
			}
		}
		
		return finalResult;
	}

}
