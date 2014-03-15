"""This module should query the real-time massdot information from Andrew Collier and return 
requested information as a json to be used by other prognostic functions."""

import pandas as pd
import urllib2 as url
import json
import sys

def ParseJson(current_transit_dict):
	"""Given a json taken from mass-dot's real-time feed (current_transit_dict), 
	return the real-time string and an appropriate data-frame with current information"""
	time_string = current_transit_dict['lastUpdated'] #when is this record taken?
	example_pair_id = current_transit_dict['pairData'].keys()[0] #grab random pair_id
	all_vars = [k for k in current_transit_dict['pairData'][example_pair_id]] #list of attributes
	output_json = {"pairId" : []} #to be structured as a flat array for easier ML manipulation
	for a in all_vars:
		output_json[a] = [] #add a list for each attribute
	for example in current_transit_dict['pairData'].keys(): #for each id/case in the current dict
		output_json["pairId"].append(example)
		for a in all_vars: #for each attribute of the example
			output_json[a].append(current_transit_dict['pairData'][example][a]) #add to the relevant column
	return time_string, pd.DataFrame(output_json)		
	
def unique(seq, keepstr=True):
	"""Given a list, return a list of all unique elements...like the 'unique' command in R"""
	"""From Jordan Callicoat, from http://code.activestate.com/recipes/502263-yet-another-unique-function/"""	
	t = type(seq)
	if t in (str, unicode):
		t = (list, ''.join)[bool(keepstr)]
	seen = []
	return t(c for c in seq if not (c in seen or seen.append(c)))

if __name__ == "__main__":
	script_name, massdot_current = sys.argv
	#where to fetch real-time data for transit:
	#massdot_current = 'http://www.acollier.com/massdot/current.json'
	req = url.Request(massdot_current)
	opener = url.build_opener()
	f = opener.open(req)
	current_time, current_data = ParseJson(json.load(f))