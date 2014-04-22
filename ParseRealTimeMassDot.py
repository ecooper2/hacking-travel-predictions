"""This module should query the real-time massdot information from Andrew Collier and return 
requested information as a json to be used by other prognostic functions."""

import pandas as pd
import urllib2 as url
import json
import sys
import BlueToadAnalysis as BTA
import NCDC_WeatherProcessor as NCDC
import datetime as dt
import numpy as np
import os

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

def GetCurrentInfo(massdot_current, DiurnalDic):
	"""To run a real-time prediction scheme, we must obtain three pieces of information.
	The first is the current weather conditions.  We have not constructed a real-time query
	to NOAA/NCDC.  This is probably above my pay-grade, but I can dig into it.  The second is
	a normalized estimate of traffic conditions.  The third is the day of the week"""
	req = url.Request(massdot_current)
	opener = url.build_opener()
	f = opener.open(req)
	current_time, current_data = ParseJson(json.load(f))
	day_of_week = BTA.GetDayOfWeek(int(NCDC.GetTimeFromDateTime(dt.datetime.now(), False)))
	time_of_day_ind = int(NCDC.GetTimeFromDateTime(dt.datetime.now(), True) * 288)
	
	pair_cond_weather_dic = {}
	for p, tt in zip(current_data.pairId, current_data.travelTime):
		#This automatically appends a ' ', indicating CLEAR weather...this must be altered
		key = p + "_" + str(day_of_week) #the key to locate the diurnal cycle for this day...	
		if key not in DiurnalDic.keys():
			pair_cond_weather_dic[p] = [0, ' ']
			print "Unable to locate key %s" % key
		else:
			pair_cond_weather_dic[p] = [float(tt) - DiurnalDic[p + "_" + str(day_of_week)][time_of_day_ind], ' ']
	
	return day_of_week, pair_cond_weather_dic

def GetRoadAveCoords(road_coord_list):
	"""Lat/Lons are provided for each pair_id in the form of lists.  This function simply returns
	the average lat and lon for each individual stretch of roadway."""
	L = len(road_coord_list) #how many coordinates are given
	return np.sum([coord[0]/L for coord in road_coord_list]), np.sum([coord[1]/L for coord in road_coord_list])
	
def GetLatLons(bt_path, file_name):
	"""This function will read, from a repository located at (bt_path), a list of the roadway
	ids (file_name), then determine an average lat/lon location to be employed to locate relevant features, i.e.
	from where shall we gather weather data.  It will then write the appropriate JSON to (bt_path)."""
	lat_lons = BTA.GetJSON(bt_path, file_name)
	RoadwayCoordsDic = {}
	for i in range(len(lat_lons['segments'])): #iterate over all roadways
		road_lat, road_lon = GetRoadAveCoords(lat_lons['segments'][i][1])
		RoadwayCoordsDic[str(int(lat_lons['segments'][i][0]))] = {"Lat" : road_lat, "Lon" : road_lon}
	with open(os.path.join(bt_path, 'RoadwayCoordsDic.txt'), 'w') as outfile:
		json.dump(RoadwayCoordsDic, outfile)
	return RoadwayCoordsDic
	
if __name__ == "__main__":
	script_name, massdot_current = sys.argv
	#where to fetch real-time data for transit:
	#massdot_current = 'http://www.acollier.com/massdot/current.json'
	req = url.Request(massdot_current)
	opener = url.build_opener()
	f = opener.open(req)
	current_time, current_data = ParseJson(json.load(f))