"""This module will generate predictions for the pair_ids contained in the BlueToad data"""

import ParseRealTimeMassDot as mass
import os
import pandas as pd
import numpy as np
import MassDotDataTypes as data
import datetime
import json
import NCDC_WeatherProcessor as NCDC

global five_minute_fractions
five_minute_fractions = [round(float(f)/288,3) for f in range(288)]

def AddDayOfWeekColumn(blue_toad, blue_toad_path, blue_toad_name):
	print "Adding days of the week for site %d" % int(blue_toad.pair_id[0:1])
	"""Given a list of YYYYDOY, return the relevant day of the week from 0-Monday, to 6-Sunday"""	
	days = [int(d) for d in blue_toad.insert_time] #remove the decimal point for time-of-day
	unique_days = mass.unique(days) #which days appear in our dataset?
	date_to_dayofweek = {}
	for date in unique_days: #each unique YYYYDOY date
		date_to_dayofweek[date] = GetDayOfWeek(date) #build hash-table connecting date to day-of-week
	#now, add the day of week column to blue_toad
	day_of_week_column = []
	for d in days:
		day_of_week_column.append(GetDayOfWeek(d))
	blue_toad['day_of_week'] = day_of_week_column
	blue_toad.to_csv(os.path.join(blue_toad_path, blue_toad_name + "_Cleaned.csv"),
						index = False)
	return blue_toad
	
def GetDayOfWeek(date):
	"""Given a YYYYDOY (2012134, e.g.) style date, return an integer from 0 (Monday) to 6 (Sunday"""
	d0 = datetime.datetime(date / 1000, 1, 1)
	real_date = d0 + datetime.timedelta(days = date % 1000 - 1, hours = 0)
	day_of_week = datetime.datetime.strptime(str(real_date.month) + ' ' + str(real_date.day) +
											', ' + str(real_date.year), "%m %d, %Y").strftime('%w')
	return (int(day_of_week) + 6) % 7 #adjusts from Monday = 1 to Monday = 0
	
def DefineDiurnalCycle(sub_bt, day, five_minute_fractions, MA_smooth_fac):
	"""Given a specific pair_id and day-of-the-week, return a diurnal cycle list, per five-minute
	interval over the the 512 5-minute intervals of the day.  (MA_smooth_fac) defines the number
	of five-minute intervals used to general the moving average.  Ex: 12 implies a one-hour total 
	window (30 minutes on either side)"""
	diurnal_cycle = []
	times_of_day = [round(i - int(i),3) for i in sub_bt.insert_time] #just the fraction of the day
	sub_bt['times_of_day'] = times_of_day
	for f in five_minute_fractions:
		sub_cycle = sub_bt[sub_bt.times_of_day == round(f,3)]
		diurnal_cycle.append(np.mean(sub_cycle.travel_time))
	return MA_Smooth_Circular(diurnal_cycle, MA_smooth_fac/2) #return after a moving-average smoothing
	
def GetSubBlueToad(bt, pair_id, day_of_week):
	"""Given a pair_id and a day_of_week, return the subset of the blue toad data."""
	sub_bt = bt[bt.pair_id == pair_id]
	sub_bt = sub_bt[sub_bt.day_of_week == day_of_week]
	return sub_bt
	
def MA_Smooth_Circular(sequence, one_dir_window):
	"""Given a (sequence), and the length, in elements of the (one_dir_window), return a smoothed
	list of similar length."""
	smoothed_seq = []
	L = len(sequence) #how long is our sequence
	for ind in xrange(L): #iterate over the sequence
		if ind < one_dir_window: #if we're too close to the beginning for a full window
			indices_at_end = one_dir_window - ind
			smoothed_seq.append(np.mean(sequence[(L-indices_at_end):] + sequence[0:(ind + one_dir_window)]))
		elif ind > L - one_dir_window: #too close to the end of the sequence for a full window
			indices_at_start = ind + one_dir_window - L
			smoothed_seq.append(np.mean(sequence[0:indices_at_start] + sequence[(ind - one_dir_window):]))
		else: #if we're in the middle somewhere
			smoothed_seq.append(np.mean(sequence[(ind - one_dir_window):(ind + one_dir_window)]))
	return smoothed_seq		
	
def GenerateDiurnalDic(bt, blue_toad_path, five_minute_fractions, window = 12):
	"""Given a cleaned blue_toad data file, produce a smoothed diurnal cycle for all pair_id,
	day_of_week combinations...and store in a dictionary for later use..."""
	#fractions of a day in 5-min increments	
	five_minute_fractions = [float(d)/24/12 for d in range(24*12)] 
	unique_ids = mass.unique(bt.pair_id)
	DiurnalDic = {}
	for u in unique_ids: #over all unique_ids
		for day in xrange(7): #over all days_of_the_week
			print "Building diurnal cycle for roadway %d on day %d (Monday = 0, Sunday = 6)" % (u,day)
			sub_bt = GetSubBlueToad(bt, u, day)
			DiurnalDic[str(u) + "_" + str(day)] = DefineDiurnalCycle(sub_bt, day, 
											 five_minute_fractions, window)
	#Write this to a .txt file as a .json
	with open(os.path.join(blue_toad_path, 'DiurnalDictionary.txt'), 'w') as outfile:
		json.dump(DiurnalDic, outfile)
	return DiurnalDic

def NormalizeTravelTime(bt, DiurnalDic, blue_toad_path, blue_toad_name):
	"""Having constructed a diurnal dictionary, for every entry in (bt), determine 
	the difference between the expected travel time from (DiurnalDic) and the actual 
	time reported by travel time. (actual - theoretical)"""
	normalized_times = [] 
	print "Now normalizing site %d" % int(bt.pair_id[0:1])
	for p, d, i, t in zip(bt.pair_id, bt.day_of_week, bt.insert_time, bt.travel_time): 
		diurnal_key = str(p) + "_" + str(d) #to find the correct key of the dictionary
		time_index = int((i - int(i)) * 288 + .0001)
		normalized_times.append(t - DiurnalDic[diurnal_key][time_index])
	bt['Normalized_t'] = normalized_times
	bt.to_csv(os.path.join(blue_toad_path, blue_toad_name + "_Cleaned" + "_Normalized.csv"),
						index = False)
	return bt

def AppendWeatherInformation(weather_data, sub_bt):
	"""For a given (sub_bt), generally subset by the relevant pair_id, append the appropriate
	weather classification from the NOAA (weather_data)"""
	bt_weather = []
	for ins_t in sub_bt.insert_time: #for each time in sub_bt, define its weather
		matched_index = (weather_data.bt_date == NCDC.RoundToNearestNth(ins_t,24,3)).nonzero()[0]
		if len(matched_index) == 0: #no matching weather date
			bt_weather.append('NONE')
		else:
			bt_weather.append(weather_data.WeatherType[int(matched_index[0])])
	sub_bt['weather'] = bt_weather
	return sub_bt
	
def SimplifyWeatherData(weather_dir, weather_data, site_name):
	"""For a given NOAA (site_name) within the (weather_dir), return the appropriately subset
	weather data."""
	weather_data = weather_data.loc[:,("Date", "Time", "WeatherType")]	
	weather_data['bt_date'] = [NCDC.ConvertWeatherDate(d, t, 24, 3) for d,t in 
							zip(weather_data.Date, weather_data.Time)]
	return weather_data

def AggregateWeather(simple_weather_data):
	"""Since NCDC lists many similar weather types, this will process a weather file 
	such that all strings containing given sub-strings are indexed immediately to 
	one given sub-grouping according to (type_dic)."""
	for ind, w in enumerate(simple_weather_data.WeatherType):
		simple_weather_data.WeatherType[ind] = NCDC.GetType(w)
	return simple_weather_data
	
def ProcessWeatherData(weather_dir, weather_site_name):
	"""Read the appropriate weather file from the (weather_dir), with the relevant 
	(weather_site_name), and remove unnecessary columns"""
	###Grab the relevant weather data
	weather_data = NCDC.GetWeatherData(weather_dir, weather_site_name)
	simple_weather_data = SimplifyWeatherData(weather_dir, weather_data, weather_site_name)
	return AggregateWeather(simple_weather_data)

def AttachWeatherData(bt, bt_path, bt_name, w_dir, w_site_name):
	"""Given a (bt) dataset and a weather directory (w_dir) in which to find the relevant
	data, process the appropriate (w_site_name), read and simplify weather data, then
	attach the appropriate column to bt and write to file."""
	print "Appending weather data for site %d" % int(bt.pair_id[0:1])
	weather_data = ProcessWeatherData(w_dir, w_site_name)
	bt = AppendWeatherInformation(weather_data, bt)
	bt.to_csv(os.path.join(bt_path, bt_name + "_Cleaned" + "_Normalized" + "_Weather.csv"), index = False)
	return bt
	
def GetAcceptableTimeRanges(time_range):
	"""Given a (time_range) in minutes, determine the appropriate five minute intervals
	peripheral to the key time that are acceptable for similar pairing.  For instance, if 
	time_range = 16, in addition to perfect matches, we are also allowed to examine 
	t + 5, t + 10, t + 15, t - 5, t - 10, and t - 15 minutes"""
	acceptable_ranges = []
	if time_range > 5: #otherwise, we can return an empty list
		for t in xrange(1,int(time_range/5)+1):
			acceptable_ranges.append(t * 5)
			acceptable_ranges.append(t * -5)
	return acceptable_ranges

def GetSub_Traffic(mini_bt, current_traffic_normalized, pct_range, L):
	"""Travel times are skewed right...i.e. the lowest normalized result is ~-600 (10 min shorter),
	but the longest normalized time is ~+10000 (3 hours longer)...thus, when looking for similar
	matches, we can consider a much larger range at the upper end than the lower end.  Thus
	function returns closest (pct_range) of the data, ensuring a relatively rich similar sample."""
	mini_bt = mini_bt.sort("Normalized_t") #easier to find percentiles
	if current_traffic_normalized >= np.max(mini_bt.Normalized_t): #above the max of the subset
		start_greater = .999*L
	else:	
		start_greater = np.min((mini_bt.Normalized_t > current_traffic_normalized).nonzero()[0])
	percentile = start_greater * 1.0 / L #which percentile of travel time are we in?
	percentile_range = (max(percentile - pct_range, 0), min(percentile + pct_range,100))
	start_ind, end_ind = int(percentile_range[0] * L), int(percentile_range[1] * L - 1)
	return mini_bt[start_ind:end_ind]
	
def GetSub_Times(traffic_sub_bt, time_range):
	"""Return a dataframe which only contains entries with the same time as the present
	or times within (time_range) of the current time. """
	traffic_sub_bt['new_ind'] = range(len(traffic_sub_bt))
	now = datetime.datetime.now() #the current instant
	current_time = NCDC.GetTimeFromDateTime(now)
	correct_times = traffic_sub_bt[traffic_sub_bt.new_ind.isin((traffic_sub_bt.time_of_day == current_time).nonzero()[0])] #only those examples at the same time_stamp
	time_shifts = GetAcceptableTimeRanges(time_range)
	if len(time_shifts) > 0: #if time_range allows for other times to be considered
		for t in time_shifts:
			new_time = NCDC.GetTimeFromDateTime(now + datetime.timedelta(minutes = t))
			correct_times = correct_times.append(traffic_sub_bt[traffic_sub_bt.new_ind.isin(
									(traffic_sub_bt.time_of_day == new_time).nonzero()[0])])
	return correct_times
	
def GetSub_DayOfWeek(mini_bt, d_o_w):
	"""Take the dataset and return only those examples occuring on the same day of the week."""
	d_o_w_bt = mini_bt[mini_bt.day_of_week == d_o_w]
	if len(d_o_w_bt) == 0:
		return mini_bt
	else:
		return d_o_w_bt
	
def GenerateNormalizedPredictions(all_pair_ids, ps_and_cs, weather_fac_dic, 
								day_of_week, pct_range, time_range, bt_path, bt_name, pcts):
	"""Iterate over all pair_ids and determine similar matches in terms of time_of_day,
	weather, traffic, and day_of_week...and generate 288 five-minute predictions (a
	24-hour prediction in 5-minute intervals)"""
	PredictionDic = {}
	for a in all_pair_ids.pair_id: #iterate over each pair_id and generate a string of predictions 
		PredictionDic[str(a)] = {}
		if str(a) in ps_and_cs.keys(): #if we have access to current conditions at this locations
			sub_bt = pd.read_csv(os.path.join(bt_path, "IndividualFiles", bt_name + "_" + str(a) + 
								"_" + "Cleaned_Normalized_Weather.csv"))
			sub_bt.index = range(len(sub_bt)) #re-index, starting from zero
			L = len(sub_bt) #how many examples, and more importantly, when does this end...

			weather_sub_bt = sub_bt[sub_bt.weather == ps_and_cs[str(a)][1]] #just similar weather
			L_w = len(weather_sub_bt)
			traffic_sub_bt = GetSub_Traffic(weather_sub_bt, ps_and_cs[str(a)][0], pct_range, L_w)
			traffic_sub_bt['time_of_day'] = [round(traffic_sub_bt.insert_time[i] - int(traffic_sub_bt.insert_time[i]),3) 
											for i in traffic_sub_bt.index]
			time_sub_bt = GetSub_Times(traffic_sub_bt, time_range * weather_fac_dic[ps_and_cs[str(a)][1]])
			day_sub_bt = GetSub_DayOfWeek(time_sub_bt, day_of_week)
			#######Generate Predictions#####
			print "Generating Predictions for site %d" % a
			for p in pcts: #iterate over the percentiles required for estimation
				pred_list = [] #will predict 5 min, 10 min, ... , 23hrs and 55min, 24 hrs
				for ind,f in enumerate(five_minute_fractions): #for generating of predictions at each forward step
					viable_future_indices = [day_sub_bt.index[i] + ind + 1 for i in xrange(len(day_sub_bt)) if i + ind + 1 < L]
					prediction_sub = sub_bt[sub_bt.index.isin(viable_future_indices)]
					if p == 'min': #if we're estimating a best case
						pred_list.append(np.min(prediction_sub.Normalized_t))	
					elif p == 'max': #if we're estimating a worst case 
						pred_list.append(np.max(prediction_sub.Normalized_t))
					else:
						pred_list.append(np.percentile(prediction_sub.Normalized_t, p))	
				PredictionDic[str(a)][str(p)] = pred_list
		else: #use default...essentially dead-average conditions, flagged as -0.00001 rather than zero
			print "No current information available for site %d, using default." % a
			pred_list = [-0.00001 for i in range(288)]
			for p in pcts: #NOTE, WITHOUT CURRENT INFO, ALL PERCENTILES WILL BE THE SAME (DEFAULT)
				PredictionDic[str(a)][str(p)] = pred_list
	with open(os.path.join(bt_path, 'CurrentPredictions.txt'), 'w') as outfile:
		json.dump(PredictionDic, outfile)
	return PredictionDic

def UnNormalizePredictions(PredictionDic, DiurnalDic, day_of_week):
	"""Turn the normalized predictions from (PredictionDic) back into the standard-form
	estimates by using (DiurnalDic)."""
	UnNormDic = {}
	for road in PredictionDic.keys(): #iterate over all pair_ids
		UnNormDic[str(road)] = {}
		for p in PredictionDic[str(road)].keys():
			std_seq = DiurnalDic[str(road) + "_" + str(day_of_week)]
			norm_seq = PredictionDic[str(road)][str(p)]
			UnNormDic[str(road)][str(p)] = [s + n for s,n in zip(std_seq, norm_seq)]
	return UnNormDic
	
def GetJSON(f_path, f_name):
	"""Read in a JSON of (f_name) at (f_path)."""
	json_data = open(os.path.join(f_path, f_name)).read()
	return json.loads(json_data)	

def HardCodedParameters():
	"""Returns a dictionary of parameters we are unlikely to change..."""
	D = {"bt_path" : os.path.join("..","..","..","Boston_Andrew","MassDothack-master", "Road_RTTM_Volume"),
	"bt_name" : "massdot_bluetoad_data",
	"bt_proc" : "no_update", "weather_site_name" : "closest", "weather_site_default" : "BostonAirport",
	"window" : 12, #how many five-minute interval defines a suitable moving-average window
	#bt_proc can be "no_update" if we are not processing/normalizing...otherwise the whole process ensues
	"pct_range" : .1, #how far from the current traffic's percentile can we deem 'similar'?
	"time_range" : 10, #how far from the current time is considered 'similar'?
	"weather_fac_dic" : {' ': 1, 'RA' : 3, 'FG' : 5, 'SN' : 10}, #how many more must we grab, by cond?
	"pct_tile_list" : ['min', 10, 20, 25, 30, 50, 70, 75, 90, 'max'], #which percentiles shall be made available, 
										#along with the best and worst-case scenarios
	"path_to_lat_lons" : "https://github.com/apcollier/hacking-travel/blob/master/js/segments.js",
	"path_to_current" : "http://traffichackers.com/current.json"}	
														
	D["weather_dir"] = os.path.join(D['bt_path'], "..", "..", "NCDC_Weather")
	return D
	
if __name__ == "__main__":
	#script_name, blue_toad_path, blue_toad_name, bt_proc, weather_site_name = sys.argv
	D = HardCodedParameters()
	if "no_update" in D['bt_proc']: #if, rather than process files, we wish to process only those files that need it
		all_pair_ids = pd.read_csv(os.path.join(D['bt_path'], "all_pair_ids.csv"))
		#all_pair_ids must exist.  The file can be shortened to only include certain roadways.
		if os.path.exists(os.path.join(D['bt_path'], "DiurnalDictionary.txt")): #if we've already generated our diurnal dict.
			DiurnalDic = GetJSON(D['bt_path'], "DiurnalDictionary.txt")
		else:
			DiurnalDic = {}
		if os.path.exists(os.path.join(D['bt_path'], "RoadwayCoordsDic.txt")):	#if we've already built it
			RoadwayCoordsDic = GetJSON(D['bt_path'], "RoadwayCoordsDic.txt")
		else:
			RoadwayCoordsDic = mass.GetLatLons(D['bt_path'], "Roadway_LatLonData.txt")
			
		for a in all_pair_ids.pair_id: #site-by-site, adding what is needed for each
			print "Processing roadway %d" % a 
			weather_site_name = NCDC.GetWSiteName(D, a, RoadwayCoordsDic) #which weather site is relevant?
			if os.path.exists(os.path.join(D['bt_path'], "IndividualFiles", D['bt_name'] + "_" +
											str(a) + "_" + "Cleaned_Normalized_Weather.csv")):
				pass #the file has been normalized with included weather
			elif os.path.exists(os.path.join(D['bt_path'], "IndividualFiles", D['bt_name'] + "_" +
											str(a) + "_" + "Cleaned_Normalized.csv")): #weather still needed.
				sub_bt = pd.read_csv(os.path.join(D['bt_path'], "IndividualFiles", 
							D['bt_name'] + "_" + str(a) + "_Cleaned_Normalized.csv"))
				sub_bt = AttachWeatherData(sub_bt, os.path.join(D['bt_path'], "IndividualFiles"), 
							D['bt_name'] + "_" + str(a), D['weather_dir'], weather_site_name)
			elif os.path.exists(os.path.join(D['bt_path'], "IndividualFiles", D['bt_name'] + "_" +
											str(a) + "_" + "Cleaned.csv")): #Normalization and weather needed.
				sub_bt = pd.read_csv(os.path.join(D['bt_path'], "IndividualFiles", 
							D['bt_name'] + "_" + str(a) + "_Cleaned.csv"))
				sub_bt = NormalizeTravelTime(sub_bt, DiurnalDic, os.path.join(D['bt_path'], "IndividualFiles"), 
										D['bt_name'] + "_" + str(a))
				sub_bt = AttachWeatherData(sub_bt, os.path.join(D['bt_path'], "IndividualFiles"), 
										D['bt_name'] + "_" + str(a), D['weather_dir'], weather_site_name)
			else:
				sub_bt = pd.read_csv(os.path.join(D['bt_path'], "IndividualFiles", 
											  D['bt_name'] + "_" + str(a) + "_Cleaned.csv"))
				sub_bt = data.CleanBlueToad(sub_bt, os.path.join(D['bt_path'], "IndividualFiles"), 
										D['bt_name'] + "_" + str(a)) #remove "/N" examples
				sub_bt = data.FloatConvert(sub_bt, os.path.join(D['bt_path'], "IndividualFiles"), 
										D['bt_name'] + "_" + str(a)) #convert strings to float where possible
				sub_bt = AddDayOfWeekColumn(sub_bt, os.path.join(D['bt_path'], "IndividualFiles"), 
										D['bt_name'] + "_" + str(a)) #0-Mon, 6-Sun
				DiurnalDic.update(GenerateDiurnalDic(sub_bt, D['bt_path'], five_minute_fractions, D['window']))
				sub_bt = NormalizeTravelTime(sub_bt, DiurnalDic, os.path.join(D['bt_path'], "IndividualFiles"), 
										D['bt_name'] + "_" + str(a))
				sub_bt = AttachWeatherData(sub_bt, os.path.join(D['bt_path'], "IndividualFiles"), 
										D['bt_name'] + "_" + str(a), D['weather_dir'], weather_site_name)
	else: #if we need to process everything
		data.GetBlueToad(D['bt_path'], D['bt_name']) #read it in and re-format dates
		all_pair_ids = pd.read_csv(os.path.join(D['bt_path'], "all_pair_ids.csv"))
		DiurnalDic = {} #To be appended, site by site
		RoadwayCoordsDic = mass.GetLatLons(D['bt_path'], "Roadway_LatLonData.txt")
		for a in all_pair_ids.pair_id: #process by site, 
			weather_site_name = NCDC.GetWSiteName(D, a, RoadwayCoordsDic) #which weather site is relevant?
			sub_bt = pd.read_csv(os.path.join(D['bt_path'], "IndividualFiles", 
											  D['bt_name'] + "_" + str(a) + "_Cleaned.csv"))
			sub_bt = data.CleanBlueToad(sub_bt, os.path.join(D['bt_path'], "IndividualFiles"), 
										D['bt_name'] + "_" + str(a)) #remove "/N" examples
			sub_bt = data.FloatConvert(sub_bt, os.path.join(D['bt_path'], "IndividualFiles"), 
										D['bt_name'] + "_" + str(a)) #convert strings to float where possible
			sub_bt = AddDayOfWeekColumn(sub_bt, os.path.join(D['bt_path'], "IndividualFiles"), 
										D['bt_name'] + "_" + str(a)) #0-Mon, 6-Sun
			DiurnalDic.update(GenerateDiurnalDic(sub_bt, D['bt_path'], five_minute_fractions, D['window']))
			sub_bt = NormalizeTravelTime(sub_bt, DiurnalDic, os.path.join(D['bt_path'], "IndividualFiles"), 
										D['bt_name'] + "_" + str(a))
			##########weather_site_name = GetWeatherSite(lat, lon) #to determine where to fetch weather conditions							
			sub_bt = AttachWeatherData(sub_bt, os.path.join(D['bt_path'], "IndividualFiles"), 
										D['bt_name'] + "_" + str(a), D['weather_dir'], weather_site_name)

	day_of_week, pairs_and_conditions = mass.GetCurrentInfo(D['path_to_current'], DiurnalDic)
	PredictionDic = GenerateNormalizedPredictions(all_pair_ids[60:75], pairs_and_conditions, D['weather_fac_dic'], 
									day_of_week, D['pct_range'], D['time_range'], 
									D['bt_path'], D['bt_name'], D['pct_tile_list'])
	CurrentPredDic = UnNormalizePredictions(PredictionDic, DiurnalDic, day_of_week)
	with open(os.path.join(D['bt_path'], 'CurrentPredictions.txt'), 'w') as outfile:
		json.dump(CurrentPredDic, outfile)

	