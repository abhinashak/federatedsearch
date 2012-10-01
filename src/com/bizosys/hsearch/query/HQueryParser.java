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

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;

public class HQueryParser {
	
	public static void main(String[] args) throws Exception {
		WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_31);
		Query qp = new QueryParser(Version.LUCENE_31,"", analyzer).parse(
			"fedQ1 AND fedQ2 OR fegQ3 OR ( fegQ5 AND fegQ6 NOT ( fegQ7 OR fegQ8 ) )  ");
		System.out.println("fedQ1.q=XXXXX&fedQ1.source=sql");
		System.out.println("fedQ2.q=YYYYYY&fedQ2.source=search");
		System.out.println("fedQ3.q=zzzzzz&fedQ3.source=dictionary");
		
		HQuery query = new HQuery();
		parseComposites(qp, query);
		System.out.println(query.toString("\n"));
	}
	
	public static HQuery parse(String query) throws Exception {
		WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_31);
		Query qp = new QueryParser(Version.LUCENE_31,"", analyzer).parse(query);
		HQuery hQuery = new HQuery();
		parseComposites(qp, hQuery);
		return hQuery;
		
	}
	
	private static void parseComposites(Query lQuery, HQuery hQuery) {
		
		for (BooleanClause clause : ((BooleanQuery)lQuery).clauses()) {
			
			Query subQueryL = clause.getQuery();

			if ( subQueryL instanceof BooleanQuery ) {

				HQuery subQueryH = new HQuery();
				subQueryH.isShould = clause.getOccur().compareTo(Occur.SHOULD) == 0; 
				subQueryH.isMust = clause.getOccur().compareTo(Occur.MUST) == 0;

				hQuery.subQueries.add(subQueryH);
				parseComposites(subQueryL, subQueryH);
			
			} else {
				HQuery.HTerm hTerm = new HQuery.HTerm();
				hTerm.isShould = clause.getOccur().compareTo(Occur.SHOULD) == 0; 
				hTerm.isMust = clause.getOccur().compareTo(Occur.MUST) == 0;
				hTerm.boost = subQueryL.getBoost();
				hQuery.terms.add(hTerm);

				if ( subQueryL instanceof TermQuery ) {
					TermQuery lTerm = (TermQuery)subQueryL;
					hTerm.type = lTerm.getTerm().field();
					hTerm.text = lTerm.getTerm().text();

				} else if ( subQueryL instanceof FuzzyQuery ) {
					FuzzyQuery lTerm = (FuzzyQuery) subQueryL;
					hTerm.isFuzzy = true;
					hTerm.type = lTerm.getTerm().field();
					hTerm.text = lTerm.getTerm().text();
				
				} else {
					hTerm.type = "Not Impemented";
					hTerm.text = "Not Impemented";
				}
				
					
			}
		}
	}
}
