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

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class FederatedExecutor implements Runnable {

	public static void search(List<IFederatedSource> sources) throws InterruptedException  {
		
		int sourcesT = sources.size();
		System.out.println("Total Sources" + sourcesT);
		if (sourcesT == 1) {
			sources.get(0).execute();
			return;
		}
		
		CountDownLatch startSignal = new CountDownLatch(1);
		CountDownLatch doneSignal = new CountDownLatch(sourcesT);
		
		for (IFederatedSource iFederatedSource : sources) {
			FederatedExecutor  aSource = new FederatedExecutor(iFederatedSource, startSignal, doneSignal);
			new Thread(aSource).start();
		}

		startSignal.countDown();
		doneSignal.await();
		
	}
	
	IFederatedSource source = null; 
	CountDownLatch startSignal = null;
	CountDownLatch doneSignal = null;

	public FederatedExecutor(IFederatedSource aSource, CountDownLatch startSignal, CountDownLatch doneSignal) {
		this.source = aSource;
		this.startSignal = startSignal;
		this.doneSignal = doneSignal;
	}
	
	public void run() {
		try {
			startSignal.await();
			source.execute();
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		} finally {
			doneSignal.countDown();
		}
	}

}
