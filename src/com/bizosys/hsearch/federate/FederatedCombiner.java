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

import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.HQuery.HTerm;
import com.bizosys.hsearch.query.HQueryCombiner;
import com.bizosys.hsearch.query.HQueryParser;
import com.bizosys.hsearch.row.IRowId;

public abstract class FederatedCombiner {

	public abstract IFederatedSource buildSource(HTerm aTerm);
	
	public List<IRowId<?,?>> combine(String query) throws Exception {
		HQuery hquery = HQueryParser.parse(query);

		List<IFederatedSource> sources = new ArrayList<IFederatedSource>();
		List<HTerm> terms = new ArrayList<HQuery.HTerm>();
		
		HQuery.toTerms(hquery, terms);
		
		for (HTerm aTerm : terms) {
			IFederatedSource fs = buildSource(aTerm);
			fs.setTerm(aTerm);
			sources.add(fs);
		}

		FederatedExecutor.search(sources);
		
		List<IRowId<?,?>> finalResult = new ArrayList<IRowId<?,?>>();
		HQueryCombiner.combine(hquery, finalResult);
		
		return finalResult;
	}
}
