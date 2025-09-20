package com.fuzzy.prometheus.client.converter.scrapeTarget;

import com.fuzzy.prometheus.client.converter.Result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultTargetResult extends Result<TargetResultItem>{
	List<TargetResultItem> activeTargets = new ArrayList<TargetResultItem>();
	List<TargetResultItem> droppedTargets = new ArrayList<TargetResultItem>();
	Map<String, String> droppedTargetCounts = new HashMap<String,String>();
	public void addActiveTarget(TargetResultItem data) {
		activeTargets.add(data);
	}
	
	public void addDroppedTarget(TargetResultItem data) {
		droppedTargets.add(data);
	}

	public void setDroppedTargetCounts(Map<String, String> droppedTargetCounts) {
		this.droppedTargetCounts = droppedTargetCounts;
	}

	@Override
	public List<TargetResultItem> getResult() {
		return activeTargets;
	}

	@Override
	public String toString() {
		return "TargetResultItem [activeTargets=" + activeTargets + ",droppedTargets="+droppedTargets+"]";
	}

}
