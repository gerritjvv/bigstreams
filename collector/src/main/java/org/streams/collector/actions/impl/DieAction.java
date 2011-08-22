package org.streams.collector.actions.impl;

import org.streams.collector.actions.CollectorAction;

public class DieAction extends CollectorAction{

	public void exec() throws Throwable{
		new Thread(){
			public void run(){
				System.exit(-1);
			}
		}.start();
	}
	
}
