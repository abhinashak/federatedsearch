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

import com.bizosys.hsearch.federate.IFederatedSource;

public class HQuery {
	boolean isShould = false;
	boolean isMust = false;
	float boost = 1.0f;
	
	public List<HQuery> subQueries = new ArrayList<HQuery>();
	public List<HTerm> terms = new ArrayList<HTerm>();
	
	public String toString(String level) {
		StringBuilder sb = new StringBuilder();
		sb.append(level).append("**********").append(":Must-");
		sb.append(isMust).append(":Should-").append( isShould).append(":Fuzzy-");
		sb.append(":Boost-").append( boost);;
		for (HQuery query : subQueries) {
			sb.append(query.toString(level + "\t"));
		}
		for (HTerm term : terms) {
			sb.append(level).append(term.type).append(":").append( term.text ).append(":Must-");
			sb.append(term.isMust).append(":Should-").append( term.isShould).append(":Fuzzy-");
			sb.append(term.isFuzzy).append(":").append( term.boost);
		}
		sb.append(level).append("**********");
		return sb.toString();
	}
	
	public static void toTerms(HQuery query, List<HTerm> terms) {
		for (HQuery subQuery : query.subQueries) {
			toTerms(subQuery, terms);
		}
		terms.addAll(query.terms);
	}

	public static class HTerm {
		public boolean isShould = false;
		public boolean isMust = false;
		public float boost = 1;
		public boolean isFuzzy = false;
	
		public String type = null;
		public String text = null;

		HResult result = null;
		public HResult getResult() throws Exception {
			if ( null != result ) return result;
			throw new Exception("Query is not executed");
		}
		
		public void setResult(HResult result) {
			this.result = result;
		}

	}
}
