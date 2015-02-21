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
import datetime
import gzip
from StringIO import StringIO

def ParseHistoricalJson(current_transit_dict):
	"""Given a json taken from mass-dot's real-time feed (current_transit_dict), 
	return the real-time string and an appropriate data-frame with current information"""
	for roadway in current_transit_dict.keys():
		var_type = type(current_transit_dict[roadway]) #anything that isn't a literal...
		if var_type != list:
			current_transit_dict[roadway] = [] #add a list for each attribute that is populated by simple variable types.
	return current_transit_dict	
	
def unique(seq, keepstr=True):
	"""Given a list, return a list of all unique elements...like the 'unique' command in R"""
	"""From Jordan Callicoat, from http://code.activestate.com/recipes/502263-yet-another-unique-function/"""	
	t = type(seq)
	if t in (str, unicode):
		t = (list, ''.join)[bool(keepstr)]
	seen = []
	return t(c for c in seq if not (c in seen or seen.append(c)))

def RetrieveJSON(path_to_massdot, json_type):	
	request = url.Request(path_to_massdot)
	request.add_header('Accept-encoding', 'gzip')	
	opener = url.build_opener()
	response = url.urlopen(request)
	if response.info().get('Content-Encoding') == 'gzip':
		buf = StringIO( response.read())		
		f = gzip.GzipFile(fileobj=buf)
	if json_type == 'historical':
		return ParseHistoricalJson(json.load(f))
	if json_type == 'current':
		return ParseCurrentJson(json.load(f))
	return None
	
def	GetDiurnalKeys_and_Indices(day_of_week, time_of_day_ind, traffic_system_memory):
	keys_and_indices = {}
	for index, steps_back in enumerate(list(range(traffic_system_memory))[::-1]):
		if steps_back <= time_of_day_ind:
			keys_and_indices[str(index)] = [str(day_of_week), time_of_day_ind - steps_back] #[which day, which index]
		else:
			if day_of_week == 0: #meaning, the day before is 6
				keys_and_indices[str(index)] = ['6', 288 - (steps_back - time_of_day_ind)]
			else:
				keys_and_indices[str(index)] = [str(day_of_week - 1), 288 - (steps_back - time_of_day_ind)]
	return keys_and_indices
	
def GetDiurnalHistory(DiurnalDic, traffic_system_memory, keys_and_indices, roadway):
	diurnal_history = []
	for value in range(traffic_system_memory):
		diurnal_key = "_".join([roadway,keys_and_indices[str(value)][0]])
		diurnal_history.append(DiurnalDic[diurnal_key][value])
	return diurnal_history		

def GetNormalizedTrafficHistory(historical_data, roadway, diurnal_history, weights, current_speed):
	L = len(diurnal_history)
	if current_speed == -1: current_speed = historical_data[roadway][-1] #just use the most recent time
	if len(historical_data[roadway]) < L - 1:
		return [0 for i in range(L)]
	else:
		return [c - d for c,d in zip(historical_data[roadway] + [current_speed], diurnal_history)]
			
def GetCurrentInfo(massdot_history, DiurnalDic, traffic_system_memory, weights, path_to_current, default_roadway):
	"""To run a real-time prediction scheme, we must obtain four pieces of information.
	The first is the current weather conditions.  We have not constructed a real-time query
	to NOAA/NCDC.  This is probably above my pay-grade, but I can dig into it.  The second is
	a normalized estimate of traffic conditions.  The third is the day of the week, the fourth
	is the time of day..."""
	historical_data = RetrieveJSON(massdot_history, 'historical')
	current_time, current_data = RetrieveJSON(path_to_current, 'current')
	current_datetime = ConvertCurrentTimeToDatetime(current_time)
	day_of_week = BTA.GetDayOfWeek(int(NCDC.GetTimeFromDateTime(current_datetime, False)))
	time_of_day_ind = int(NCDC.GetTimeFromDateTime(current_datetime, True) * 288)
	keys_and_indices = GetDiurnalKeys_and_Indices(day_of_week, time_of_day_ind, traffic_system_memory)
	pair_cond_weather_dic = {}
	for roadway in [k for k in historical_data.keys() if k != 'Start']:
		if str(roadway) + "_" + str(day_of_week) not in DiurnalDic.keys(): 
			DiurnalDic = AddDummyValuesToDiurnalDic(DiurnalDic, day_of_week, roadway, default_roadway)
		print "gathering current and recent conditions for roadway %s" % roadway
		if roadway not in current_data.keys():
			print "No recent data available for roadway %s" % roadway
			if roadway + "_0" not in DiurnalDic.keys():
				print "No historical data available for roadway %s" % roadway
				current_speed = -1
				normalized_history = [0 for w in weights] #assume 'typical conditions'
			else:	
				current_speed = DiurnalDic[roadway + "_" + str(day_of_week)][time_of_day_ind]
				diurnal_history = GetDiurnalHistory(DiurnalDic, traffic_system_memory, keys_and_indices, roadway)
				normalized_history = GetNormalizedTrafficHistory(historical_data, roadway, diurnal_history, weights, current_speed)
		else:
			current_speed = float(current_data[roadway]['speed']) if not current_data[roadway]['stale'] else DiurnalDic[roadway + "_" + str(day_of_week)][time_of_day_ind]	
			diurnal_history = GetDiurnalHistory(DiurnalDic, traffic_system_memory, keys_and_indices, roadway)
			normalized_history = GetNormalizedTrafficHistory(historical_data, roadway, diurnal_history, weights, current_speed)
		pair_cond_weather_dic[roadway] = [np.sum([n * w for n,w in zip(normalized_history, weights)]), ' ', current_speed]
	return day_of_week, current_datetime, pair_cond_weather_dic
	
def	AddDummyValuesToDiurnalDic(DiurnalDic, day_of_week, roadway, default_roadway):
	diurnal_key = roadway + "_" + str(day_of_week)
	if not diurnal_key in DiurnalDic.keys():
		DiurnalDic[diurnal_key] = DiurnalDic[str(default_roadway) + "_" + str(day_of_week)]
	return DiurnalDic
			
def ParseCurrentJson(current_transit_dict): 
	"""Given a json taken from mass-dot's real-time feed (current_transit_dict),  
	return the real-time string and an appropriate data-frame with current information""" 
	time_string = current_transit_dict['lastUpdated'] #when is this record taken? 
	output_json = {} #to be structured as a flat array for easier ML manipulation 
 	for roadway in current_transit_dict['pairData'].keys(): #for each id/case in the current dict 
		roadway_info = current_transit_dict['pairData'][roadway]
		output_json[roadway] = {'speed' : roadway_info['speed'], 'stale' : roadway_info['stale']}
 	return time_string, output_json 
	
def ConvertCurrentTimeToDatetime(current_time):
	"""Given a (current_time), convert this string into the format associated with the datetime functions
	used to manipulate times and dates.  Typically, this tuple is (year, month, day, hour, minute, second, ...)"""

	time_zone_position = current_time.find('GMT')
	current_time_filtered = current_time[0:time_zone_position-1]
	return datetime.datetime.strptime(current_time_filtered, "%a %b %d %Y %H:%M:%S")	
	
        '''
	month_dic = {'Jan' : 1, 'Feb' : 2, 'Mar' : 3, 'Apr' : 4, 'May': 5, 'Jun' : 6, 'Jul' : 7, 'Aug' : 8,
				 'Sep' : 9, 'Oct' : 10, 'Nov' : 11, 'Dec' : 12} #ANDREW, VERIFY THESE ARE YOUR PREFIXES!
	date, time, time_zone = current_time.split() #currently ignore time_zone, assumed to be GMT
	month, day, year = date.split('-'); 
	hour, minute, second = time.split(':')
	return dt.datetime(int(year), month_dic[month], int(day), int(hour), int(minute), int(second))
	'''

def YYYYDOY_to_Datetime(date):
	"""Given a (date) of the YYYYDOY format, convert this to a datetime format"""
	year = int(date)/1000
	day = int(date - year*1000)
	day_frac = NCDC.RoundToNearestNth(date - int(date), 288, 3) #closest five-minute mark
	mins_into_day = int(day_frac * 1440 / 5 + 0.5)*5 #how many minutes into the day?
	leap_years = [1900 + 4*x for x in range(50)] #runs until 2096 for potential leap_years
	days_in_month = np.cumsum([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]) #to covert into day_of_year		
	if year in leap_years: 
		days_in_month[1:] += 1
	for i,d in enumerate(days_in_month):
		if d > day and i > 0:
			month = i+1; day_of_month = day - days_in_month[i-1] + 1
			break	
		elif d > day:
			month = i+1; day_of_month = day + 1
			break
	hour = mins_into_day/60
	minute = mins_into_day - hour * 60
	return dt.datetime(year, month, day_of_month, hour, minute, 0)
	
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
	with open(os.path.join(bt_path, 'RoadwayCoordsDic.txt'), 'wb') as outfile:
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
