package com.fuzzy.prometheus.client.converter.am;

import com.fuzzy.prometheus.client.converter.Result;

import java.util.ArrayList;
import java.util.List;

public class DefaultAlertManagerResult extends Result<AlertManagerResultItem>{
	List<AlertManagerResultItem> activeAlertmanagers = new ArrayList<AlertManagerResultItem>();
	List<AlertManagerResultItem> droppedAlertmanagers = new ArrayList<AlertManagerResultItem>();
	public void addActiveManager(AlertManagerResultItem data) {
		activeAlertmanagers.add(data);
	}
	
	public void addDroppedManager(AlertManagerResultItem data) {
		droppedAlertmanagers.add(data);
	}
	
	@Override
	public List<AlertManagerResultItem> getResult() {
		return activeAlertmanagers;
	}

	@Override
	public String toString() {
		return "TargetResultItem [activeAM=" + activeAlertmanagers + ",droppedAM="+droppedAlertmanagers+"]";
	}	

}
