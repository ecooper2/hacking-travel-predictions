"""This module will generate predictions for the pair_ids contained in the BlueToad data"""

import ParseRealTimeMassDot as mass
import os
import pandas as pd
import numpy as np
import MassDotDataTypes as data
import datetime
import json
import NCDC_WeatherProcessor as NCDC
import math
import zipfile as Z
import sys
import argparse
from urllib2 import urlopen, URLError, HTTPError

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
	real_date = d0 + datetime.timedelta(days = date % 1000, hours = 0)
	day_of_week = datetime.datetime.strptime(str(real_date.month) + ' ' + str(real_date.day) +
											', ' + str(real_date.year), "%m %d, %Y").strftime('%w')
	return (int(day_of_week) + 6) % 7 #adjusts from Monday = 1 to Monday = 0

def DefineDiurnalCycle(sub_bt, day, five_minute_fractions, MA_smooth_fac, def_val):
	"""Given a specific pair_id and day-of-the-week, return a diurnal cycle list, per five-minute
	interval over the the 288 5-minute intervals of the day.  (MA_smooth_fac) defines the number
	of five-minute intervals used to general the moving average.  Ex: 12 implies a one-hour total
	window (30 minutes on either side).  (def_val) is a default value in the absence of other info."""
	diurnal_cycle = []
	times_of_day = [round(i - int(i),3) for i in sub_bt.insert_time] #just the fraction of the day
	sub_bt['times_of_day'] = times_of_day
	for f in five_minute_fractions:
		sub_cycle = sub_bt[sub_bt.times_of_day == round(f,3)] if len(sub_bt) > 0 else []
		if len(sub_cycle) == 0: #meaning there are no examples on this day of the week at this time
			if len(diurnal_cycle) == 0:
				diurnal_cycle.append(def_val) #assume the previous five-minute timestamp's time
			else:
				diurnal_cycle.append(diurnal_cycle[-1]) #with no other information default to the mean time at that roadway
		else:
			diurnal_cycle.append(np.mean(sub_cycle.speed))
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
			DiurnalDic[str(u) + "_" + str(day)] = DefineDiurnalCycle(sub_bt, day, five_minute_fractions, 
														window, np.mean(bt.speed)) #default to mean
	return DiurnalDic

def NormalizeTravelTime(bt, DiurnalDic, blue_toad_path, blue_toad_name):
	"""Having constructed a diurnal dictionary, for every entry in (bt), determine
	the difference between the expected travel time from (DiurnalDic) and the actual
	time reported by travel time. (actual - theoretical)"""
	normalized_times = []
	if len(bt) > 0: 
		print "Now normalizing site %d" % int(bt.pair_id[0:1])
		for p, d, i, t in zip(bt.pair_id, bt.day_of_week, bt.insert_time, bt.speed):
			diurnal_key = str(p) + "_" + str(d) #to find the correct key of the dictionary
			time_index = int((i - int(i)) * 288 + .0001)
			normalized_times.append(round(t - DiurnalDic[diurnal_key][time_index], 2))
		bt['Normalized_t'] = normalized_times
	else:
		bt['Normalized_t'] = []
	bt.to_csv(os.path.join(blue_toad_path, blue_toad_name + "_Cleaned" + "_Normalized.csv"), index = False)
	return bt

def AppendWeatherInformation(weather_data, sub_bt):
	"""For a given (sub_bt), generally subset by the relevant pair_id, append the appropriate
	weather classification from the NOAA (weather_data)"""
	bt_weather = []
	for ins_t in sub_bt.insert_time: #for each time in sub_bt, define its weather
		matched_index = (weather_data.bt_date == NCDC.RoundToNearestNth(ins_t,24,3)).nonzero()[0]
		if len(matched_index) == 0: #no matching weather date
			bt_weather.append(' ') #Fair warning here...in the absence of weather, assume clear skies
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
	if len(bt) > 0:
		print "Appending weather data for site %d" % int(bt.pair_id[0:1])
		weather_data = ProcessWeatherData(w_dir, w_site_name)
		bt = AppendWeatherInformation(weather_data, bt)
	else:
		bt['weather'] = []
	bt.to_csv(os.path.join(bt_path, bt_name + "_Cleaned" + "_Normalized" + "_Weather.csv"), index = False)
	return bt

def AttachTrafficHistory(sub_bt, bt_path, bt_name, D, weights):	
	traffic_history = []; historical_window = D['traffic_system_memory']
	if len(sub_bt) > 0:
		print "Appending traffic history for site %d" % int(sub_bt.pair_id[0:1])
		for i in range(len(sub_bt)):
			if i == 0:
				traffic_history.append(0)
			else:
				traffic_history.append(CalculateAntecedentTraffic(sub_bt.Normalized_t[max(0,i-historical_window):i][::-1], weights[0:historical_window], historical_window))
		sub_bt['norm_traffic_hist'] = traffic_history
	else:
		sub_bt['norm_traffic_hist'] = []
	sub_bt.to_csv(os.path.join(bt_path, bt_name + "_CNW_TrafficHist.csv"), index = False)
	return sub_bt
			
def AttachWeatherHistory(sub_bt, bt_path, bt_name, D, weights):
	weather_history = []; historical_window = D['traffic_system_memory']
	if len(sub_bt) > 0:
		print "Appending weather history for site %d" % int(sub_bt.pair_id[0:1])
		for i in range(len(sub_bt)):
			if i == 0:
				weather_history.append(0)
			else:
				weather_history.append(CalculateAntecedentWeather(sub_bt.weather[max(0,i-historical_window):i][::-1],weights[0:historical_window],D['weather_cost_facs'], historical_window))
		sub_bt['weather_hist'] = weather_history
	else:
		sub_bt['weather_hist'] = []
	sub_bt.to_csv(os.path.join(bt_path, bt_name + '_CNW_TrafficHist_WeatherHist.csv'), index = False)
	return sub_bt

def CalculateAntecedentWeather(weather_history, weights, weather_cost_facs, historical_window):
	if len(weather_history) == historical_window:
		return np.sum([weather_cost_facs[weather]*w for weather,w in zip(weather_history, weights)])
	else:
		normalized_weights = NormalizeWeights(weights[0:len(weather_history)], np.sum(weights))
		return np.sum([weather_cost_facs[weather]*w for weather,w in zip(weather_history, normalized_weights)])		

def CalculateAntecedentTraffic(norm_traffic_history, weights, historical_window):
	if len(norm_traffic_history) == historical_window:
		return np.sum([norm_traffic * weight for norm_traffic, weight in zip(norm_traffic_history, weights)])
	else: 
		normalized_weights = NormalizeWeights(weights[0:len(norm_traffic_history)], np.sum(weights))
		return np.sum([norm_traffic * weight for norm_traffic, weight in zip(norm_traffic_history, normalized_weights)])
	
def NormalizeWeights(weights, norm_sum):
	return [float(w)/norm_sum for w in weights]
	
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

def GetSub_Traffic(mini_bt, current_traffic_normalized, pct_range, L, traffic_column):
	"""Travel times are skewed right...i.e. the lowest normalized result is ~-600 (10 min shorter),
	but the longest normalized time is ~+10000 (3 hours longer)...thus, when looking for similar
	matches, we can consider a much larger range at the upper end than the lower end.  Thus
	function returns closest (pct_range) of the data, ensuring a relatively rich similar sample."""
	mini_bt = mini_bt.sort(traffic_column) #easier to find percentiles
	if current_traffic_normalized >= np.max(mini_bt[traffic_column]): #above the max of the subset
		start_greater = .999*L
	else:
		start_greater = np.min((mini_bt[traffic_column] > current_traffic_normalized).nonzero()[0])
	percentile = start_greater * 1.0 / L #which percentile of travel time are we in?
	percentile_range = (max(percentile - pct_range, 0), min(percentile + pct_range,1))
	start_ind, end_ind = int(percentile_range[0] * L), int(percentile_range[1] * L - 1)
	return mini_bt[start_ind:end_ind]

def GetCorrectDaytimes(traffic_sub_bt, day_of_week, current_time, subset, analysis_day):
	"""Given the similar traffic examples in (traffic_sub_bt), the (day_of_week), the (current_time), and
	descriptions in (subset) of whether we are interested in the same day or simply weekday/weekend matches,
	return the list of daytimes that are appropriate."""
	if analysis_day >= 0: #if we're working our way down to a specific day-of-the-week
		return traffic_sub_bt[np.logical_and(np.logical_and(traffic_sub_bt.day_of_week == analysis_day,
									  traffic_sub_bt.time_of_day < current_time + 0.001), 
									  traffic_sub_bt.time_of_day > current_time - 0.001)], [analysis_day]

	else: #'S' in subset, grab weekday or weekends
		if 'S' in subset: #it's a weekend
			return traffic_sub_bt[np.logical_and(np.logical_and(traffic_sub_bt.day_of_week >= 5,
									  traffic_sub_bt.time_of_day < current_time + 0.001), 
									  traffic_sub_bt.time_of_day > current_time - 0.001)], [5,6]
		else: #it's a weekday
			return traffic_sub_bt[np.logical_and(np.logical_and(traffic_sub_bt.day_of_week < 5,
									  traffic_sub_bt.time_of_day < current_time + 0.001), 
									  traffic_sub_bt.time_of_day > current_time - 0.001)], [0,1,2,3,4]

def GetSub_Times_and_Days(traffic_sub_bt, current_datetime, subset, time_of_day, time_range, day_of_week, analysis_day = -1):
	"""Return a dataframe which only contains entries with the same time as the present
	or times within (time_range) of the current time on the appropriate days of the week.
	For instance, 11:00pm times could include 12:30am as 'similar' examples.  These would be on the
	following day.  If 'S' in (subset), we will return days 0-4, weekdays, or 5-6, weekends.
	If (analysis_day) assumes a non-negative value, use this day's data only"""
	if time_of_day == "": #if the users have not insisted on a time of day
		current_time = NCDC.GetTimeFromDateTime(current_datetime) #0-1, three decimal time of day (.875, e.g.)
	else:
		current_time, time_range = time_of_day, 0
	current_day = current_datetime.day #day of the month (allows distinguishing similar times on other days)
	correct_daytimes, viable_days = GetCorrectDaytimes(traffic_sub_bt, day_of_week, current_time, subset, analysis_day)

	time_shifts = GetAcceptableTimeRanges(time_range)
	if len(time_shifts) > 0: #if time_range allows for other times to be considered
		for d in viable_days: #checking each day, even if there is only one...
			shift = d - day_of_week #how many days different is this from the current day?
			testing_datetime = current_datetime + datetime.timedelta(days = shift)
			for t in time_shifts:
				new_datetime = testing_datetime + datetime.timedelta(minutes = t)
				new_day = new_datetime.day #check to see if this 'similar' point is a previous/subsequent day
				new_time = NCDC.GetTimeFromDateTime(new_datetime)
				shift_day_of_week = LinDayOfWeekShift(day_of_week, shift)
				new_day_of_week = AdjustDayOfWeek(testing_datetime.day, new_day, shift_day_of_week) #did we move into a new day?
				correct_daytimes = correct_daytimes.append(traffic_sub_bt[np.logical_and(np.logical_and(traffic_sub_bt.day_of_week == new_day_of_week,
										  traffic_sub_bt.time_of_day < new_time + .001), traffic_sub_bt.time_of_day > new_time - .001)])
	return correct_daytimes

def LinDayOfWeekShift(day_of_week, shift):
	"""Given a (day_of_week), numbered 0-6, and a shift, -2, +3, e.g., return the new day of week."""
	if shift == 0: #no change
		return day_of_week
	else: #a shift (positive or negative)
		return (day_of_week + shift) % 7

def AdjustDayOfWeek(current_day, new_day, day_of_week):
	"""Given the (current_day), a (new_day) that represents the date of the similar historical example,
	which could be a previous or subsequent day, and the current (day_of_week), measured 0-Mon to 6-Sun,
	return the appropriate day of the week for (new_day)."""
	if new_day == current_day: #if we are still considering the same day.
		return day_of_week
	elif new_day - current_day == 1: #step forward one day
		return (day_of_week + 1) % 7 #if we are stepping forward on a Sunday, Monday resets to day 0
	elif new_day - current_day == -1: #step backward one day
		return (day_of_week + 6) % 7 #if we are stepping backward on a Monday, Sunday jumps to day 6
	elif new_day - current_day > 1: #moving from the 1st of a month to the 31st of the preceding month
		return (day_of_week + 6) % 7 #to ensure a step back from Monday is Sunda7
	elif new_day - current_day < 1: # moving from the 30th/31st of a month to the 1st of the succeeding month
		return (day_of_week + 1) % 7

def	AddEmptyDic(a, pcts, PredictionDic):
	"""For a given roadway (a), a list of percentages to consider (pcts), and a (PredictionDic),
	fill each index with empty lists..."""
	print "Insufficient data for site %d" % a
	for p in pcts:				#we cannot expand the dataset...and so we should return an empty list.
		PredictionDic[str(a)][str(p)] = []
	return PredictionDic	

def NoPrediction(all_pair_ids, D):
	"""In this case, we are not generating predictions, but rather reporting the historical values
	in json format."""	
	ReportDictionary = {}
	for a in all_pair_ids.pair_id:
		print 'Reporting times (without prediction) for roadway %d' % a
		ReportDictionary[str(a)] = {'speed' : [], 'insert_time' : []}
		sub_bt = pd.read_csv(os.path.join(D['update_path'], "IndividualFiles", D['bt_name'] + "_" + str(a) +
								"_" + "Cleaned_Normalized_Weather.csv")).fillna(' ')
		sub_bt = sub_bt[np.logical_and(sub_bt.insert_time >= D['start_date'], sub_bt.insert_time <= D['end_date'])]
		for i,t in zip(sub_bt.insert_time, sub_bt.speed): 
			if type(t) == str: #missing time...
				ReportDictionary[str(a)]['speed'].append('null')
			else:
				ReportDictionary[str(a)]['speed'].append(min(float(t), D['max_speed']))
			ReportDictionary[str(a)]['insert_time'].append(mass.YYYYDOY_to_Datetime(i).isoformat())	
	ReportDictionary['Start'] = mass.YYYYDOY_to_Datetime(D['start_date']).isoformat()
	ReportDictionary['End'] = mass.YYYYDOY_to_Datetime(D['end_date']).isoformat()
	return ReportDictionary	
	
def GenerateNormalizedPredictions(all_pair_ids, ps_and_cs, weather_fac_dic, day_of_week, current_datetime, pct_range, 
								  time_range, bt_path, bt_name, pcts, subset, pred_len, time_of_day, weather_kernel_pct,
								  start_date, end_date, use_traffic_hist = True):
	"""Iterate over all pair_ids and determine similar matches in terms of time_of_day,
	weather, traffic, and day_of_week...and generate 288 five-minute predictions (a
	24-hour prediction in 5-minute intervals)"""
	traffic_column = 'norm_traffic_hist' if use_traffic_hist else 'Normalized_t'
	weather_severity_fac, min_matches, min_weather_kernel_size, min_traffic_bt_size = 2.5, 10, 2, 150 #change if needed
	min_traffic_kernel_size = 50 #change if needed
	
	PredictionDic = {}
	for a in all_pair_ids.pair_id: #iterate over each pair_id and generate a string of predictions
		PredictionDic[str(a)] = {}
		if str(a) in ps_and_cs.keys(): #if we have access to current conditions at this locations
			sub_bt = pd.read_csv(os.path.join(bt_path, "IndividualFiles", bt_name + "_" + str(a) +
								"_" + "CNW_TrafficHist_WeatherHist.csv")).fillna(' ')
			sub_bt = sub_bt[np.logical_and(sub_bt.insert_time >= start_date, sub_bt.insert_time <= end_date)]
			L = len(sub_bt) #how many examples, and more importantly, when does this end...
			sub_bt.index = range(L) #re-index, starting from zero
			if 'W' in subset:
				weather_kernel_size = max(ps_and_cs[str(a)][1] * weather_kernel_pct, min_weather_kernel_size)
				weather_sub_bt = sub_bt[np.logical_and(sub_bt.weather_hist <= ps_and_cs[str(a)][1] + weather_kernel_size,
													   sub_bt.weather_hist >= ps_and_cs[str(a)][1] - weather_kernel_size)]
			else:
				weather_sub_bt = sub_bt
			L_w = len(weather_sub_bt)
			if L_w == 0:
				print "NO HISTORICAL EXAMPLES OF THIS WEATHER TYPE AT ROADWAY %d." % a
				PredictionDic = AddEmptyDic(a, pcts, PredictionDic) #Fill with empty lists
			if 'T' in subset and PredictionDic[str(a)] == {}:  
				traffic_sub_bt = GetSub_Traffic(weather_sub_bt, ps_and_cs[str(a)][0], pct_range, L_w, traffic_column)
				if len(traffic_sub_bt) < min_traffic_bt_size:
					traffic_sub_bt = GetSub_Traffic(weather_sub_bt, ps_and_cs[str(a)][0], pct_range * 2, L_w, traffic_column)
			else:
				traffic_sub_bt = weather_sub_bt
			#locate similar days/times, more lax search in less common weather
			if ('Y' in subset or 'S' in subset) and PredictionDic[str(a)] == {}: #if we need to choose only certain days of the week
				day_sub_bt	= GetSub_Times_and_Days(traffic_sub_bt, current_datetime, subset, time_of_day,
								time_range * max(int(ps_and_cs[str(a)][1]/weather_severity_fac),1), day_of_week)
				if len(day_sub_bt) < min_matches: #if our similarity requirements are too stringent
					day_sub_bt = RelaxRequirements_GetMatches(traffic_sub_bt, current_datetime, subset, time_of_day, time_range, 
								ps_and_cs, a, weather_severity_fac, day_of_week, min_matches)
					if len(day_sub_bt) < min_matches:
						PredictionDic = AddEmptyDic(a, pcts, PredictionDic) #Fill with empty lists
			elif ('0' in subset or '1' in subset or '2' in subset or '3' in subset 
				  or '4' in subset or '5' in subset or '6' in subset) and PredictionDic[str(a)] == {}: #it's a specific day-of-week
				day_sub_bt = GetSub_Times_and_Days(traffic_sub_bt, current_datetime, subset, time_of_day,
								time_range * max(int(ps_and_cs[str(a)][1]/weather_severity_fac),1), day_of_week, int(subset[-1]))
				if len(day_sub_bt) < min_matches: #if our similarity requirements are too stringent
					day_sub_bt = RelaxRequirements_GetMatches(traffic_sub_bt, current_datetime, subset, time_of_day, time_range, 
								ps_and_cs, a, weather_severity_fac, day_of_week, min_matches)
					if len(day_sub_bt) < min_matches:
						PredictionDic = AddEmptyDic(a, pcts, PredictionDic) #Fill with empty lists
			else:
				day_sub_bt = traffic_sub_bt
			if len(day_sub_bt) > min_matches: 
				for p in pcts:
					PredictionDic[str(a)][str(p)] = [] #will predict 5 min, 10 min, ... , 23hrs and 55min, 24 hrs
								  #######Generate Predictions#####
				print "Generating Predictions for site %d with a subset of length %d" % (a, len(day_sub_bt))
				for ind in xrange(pred_len): #for generating of predictions at each forward step
					viable_future_indices = [day_sub_bt.index[i] + ind + 1 for i in xrange(len(day_sub_bt)) if day_sub_bt.index[i] + ind + 1 < L]
					prediction_sub = sub_bt.iloc[viable_future_indices]
					if len(prediction_sub) > 0:
						for p in pcts: #iterate over the percentiles required for estimation
							if p == 'min': #if we're estimating a best case
								PredictionDic[str(a)][str(p)].append(np.min(prediction_sub.speed))
							elif p == 'max': #if we're estimating a worst case
								PredictionDic[str(a)][str(p)].append(np.max(prediction_sub.speed))
							else:
								PredictionDic[str(a)][str(p)].append(np.percentile(prediction_sub.speed, p))
					else:
						for p in pcts:
							PredictionDic[str(a)][str(p)].append(PredictionDic[str(a)][str(p)][-1])
			else:
				print "no predictions generated for %d" % a
		else: #use default...essentially dead-average conditions, flagged as -0.00001 rather than zero
			PredictionDic = DefaultPredictions(a, D, pcts, PredictionDic)
	#with open(os.path.join(bt_path, 'CurrentPredictions.txt'), 'wb') as outfile:
	#	json.dump(PredictionDic, outfile)
	return PredictionDic

def RelaxRequirements_GetMatches(traffic_sub_bt, current_datetime, subset, time_of_day, time_range, 
								ps_and_cs, a, weather_severity_fac, day_of_week, min_matches):	
	###Step 1: convert from a specific day 'e.g. Tuesday' to a more general classification 'e.g. weekday'
	if subset[-1] in ['5','6']:
		subset = subset.replace('O','S')
	elif subset[-1] in ['0','1','2','3','4','5']:
		subset = subset.replace('O','Y')
	matches = GetSub_Times_and_Days(traffic_sub_bt, current_datetime, subset, time_of_day,
								time_range * max(int(ps_and_cs[str(a)][1]/weather_severity_fac),1), day_of_week)
	if len(matches) > min_matches:
		return matches
	else:
		###Step 2: flex the temporal requirements so twice as many matches are plausible.
		matches = GetSub_Times_and_Days(traffic_sub_bt, current_datetime, subset, time_of_day,
								time_range * 2 * max(int(ps_and_cs[str(a)][1]/weather_severity_fac),1), day_of_week)
		if len(matches) > min_matches:
			return matches
		else:
			###Step 3: flex the temporal requirements to once again double matches...
			matches = GetSub_Times_and_Days(traffic_sub_bt, current_datetime, subset, time_of_day,
						time_range * 4 * max(int(ps_and_cs[str(a)][1]/weather_severity_fac),1), day_of_week)
			if len(matches) > min_matches:
				return matches
			else:
				###Step 4: flex the requirements by a factor of two once more
				matches = GetSub_Times_and_Days(traffic_sub_bt, current_datetime, subset, time_of_day,
						time_range * 8 * max(int(ps_and_cs[str(a)][1]/weather_severity_fac),1), day_of_week)
				return matches
	
def DefaultPredictions(a, D, pcts, PredictionDic):
	"""For a given roadway (a) and parameter dictionary (D), update all percentiles (pct) in (PredictionDict)
	with default predictions."""
	print "No current information available for site %d, using default." % a
	pred_list = [-0.00001 for i in range(D['pred_duration'])]
	for p in pcts: #NOTE, WITHOUT CURRENT INFO, ALL PERCENTILES WILL BE THE SAME (DEFAULT)
		PredictionDic[str(a)][str(p)] = pred_list
	return PredictionDic
	
def RoundToFive(current_datetime):
	"""Rounds a (current_datetime) to the nearest multiple of 5."""
	minute = current_datetime.minute; second = current_datetime.second
	rounded_minute = int(float(minute)/5 + float(second)/60/5 + .5)*5
	if rounded_minute == 60:
		current_datetime = current_datetime.replace(minute = 0, second = 0)
		current_datetime = current_datetime + datetime.timedelta(minutes = 60)
	else:
		current_datetime = current_datetime.replace(minute = rounded_minute, second = 0)
	return current_datetime
	
def UnNormalizePredictions(PredictionDic, DiurnalDic, MaximumDic, day_of_week, 
							current_datetime, pred_len, time_of_day, max_speed, ps_and_cs, smoother):
	"""Turn the normalized predictions from (PredictionDic) back into the standard-form
	estimates by using (DiurnalDic)."""
	UnNormDic = {}
	if time_of_day == '': #meaning this was not explicitly set 
		UnNormDic['Start'] = RoundToFive(current_datetime).isoformat() #current time, each prediction is 5,10,...minutes after
	else:
		minutes_into_day = round(time_of_day * 288, 0) * 5
		h = int(minutes_into_day / 60); m = int(minutes_into_day - 60 * h)
		current_datetime = current_datetime.replace(hour = h, minute = m, second = 0) #set this to the user-input start_time
		UnNormDic['Start'] = current_datetime.isoformat()
	for road in PredictionDic.keys(): #iterate over all pair_ids
		max_speed = MaximumDic[road] #shortest historical travel time for a roadway
		UnNormDic[str(road)] = {}
		if str(road) in ps_and_cs.keys():
			for p in PredictionDic[str(road)].keys():
				norm_seq = PredictionDic[str(road)][str(p)]
				if len(norm_seq) == 0: 
					UnNormDic[str(road)][str(p)] = []
				else:
					UnNormDic[str(road)][str(p)] = [min(n, max_speed)* float(min(smoother, i+1))/smoother + float(max(0,smoother-i-1))/smoother * float(ps_and_cs[str(road)][2]) for i,n in enumerate(norm_seq)]
		else:
			for p in PredictionDic[str(road)].keys():
				UnNormDic[str(road)][str(p)] = None
	return UnNormDic

def PctMap(pct_keys):
	"""Given a list of (pct_keys), of the form '10', '20', 'min', 'max', etc, reverse them to map
	travel_times to speeds, which are inversely related."""
	pct_map = {}
	for key in pct_keys:
		if key in ['min', 'max']:
			pct_map[key] = 'min' if key == 'max' else 'max'
		else:
			pct_map[key] = str(100 - int(key))
	return pct_map
	
def NDigitString(n, num):
	"""Given a prescribed length of a digit string (n), turn a number (num) into the appropriate length
	string with leading zeroes.  For instance, NDigitString(3, 12) should return '012', NDigitString(3,9)
	should return '009', and so on."""
	if num == 0 or num < 0: #cannot take logs of non-positive values
		n_digits = 1
	else:
		n_digits = int(math.log(num,10)) + 1 #how many digits are found in the number?
	if n_digits >= n:
		return str(num) #no need to add leading zeros
	else:
		return '0' * (n - n_digits) + str(num) #add the necessary leading zeros and return

def GetStandardSequence(road, day_of_week, current_datetime, DiurnalDic, pred_len):
	"""Given the (road), (day_of_week), the (current_datetime) at which we are looking to make predictions
	forward in time, and the (DiurnalDic) used for baseline expectations, return the (pred_len) next baseline
	travel times.  Note, if we are beginning at 12pm on Sunday, within 24 hours, we will be making
	predictions for the subsequent Monday morning, a different list from DiurnalDic"""
	current_Diurnal_key = str(road) + "_" + str(day_of_week); fixed_pred_len = pred_len #this will not be changed
	if current_Diurnal_key not in DiurnalDic.keys(): 
		print "No data from segment %s historically on day %s, returning an empty list." % (str(road), str(day_of_week))
		return []
	day_index = GetIndexFromDatetime(current_datetime) #how many 5-minute intervals are we into the day
	future_days = (day_index + pred_len)/288
	d_seq = [] #to be extended with each iteration of the loop
	for d in range(future_days + 1): #how many days in the DiurnalDic must we consider?
		if d == future_days and pred_len > 0: #if this the last day for which we need to access DiurnalDic
			d_seq = d_seq + DiurnalDic[current_Diurnal_key][(day_index + 1):(day_index + pred_len - 1)]
		elif pred_len > 0: #add another day of normalized estimates and step forward one day
			new_datetime = current_datetime + datetime.timedelta(minutes = 1440) #the 'next' day
			new_day_of_week = AdjustDayOfWeek(current_datetime.day, new_datetime.day, day_of_week) #which day of the week?
			next_Diurnal_key = str(road) + "_" + str(new_day_of_week) #the DiurnalDic key for next-day predictions
			if next_Diurnal_key not in DiurnalDic.keys(): 
				print "No data from segment %s historically on day %s, defaulting to a static, 100s transit time." % (str(road), str(day_of_week))
				return []
			d_seq = d_seq + DiurnalDic[current_Diurnal_key][(day_index + 1):] + DiurnalDic[next_Diurnal_key][0:day_index+1]
			#step all values forward one day and remove 288 steps from pred_len
			day_of_week = new_day_of_week; current_datetime = new_datetime; current_Diurnal_key = next_Diurnal_key; pred_len -= 288
		else:
			pass
	return d_seq[:fixed_pred_len]

def GetIndexFromDatetime(current_datetime):
	"""Given the (current_datetime), which is a datetime object, return an index from 0 to 287,
	denoting the five-minute interval of the day at which we begin, rounding down.  For instance,
	at 12:03am, this should return a 0, at 6:03am, this should return 72, and at 11:57pm, this should
	return 288."""
	return (current_datetime.hour)*12 + int(current_datetime.minute/5)

def GetJSON(f_path, f_name):
	"""Read in a JSON of (f_name) at (f_path)."""
	json_data = open(os.path.join(f_path, f_name)).read()
	return json.loads(json_data)

def DefineMaximums(D, all_pair_ids):
	"""To avoid predictions of unrealistically high travel speeds, given a dictionary (D) of parameters,
	and a list of (all_pair_ids), return the maximum recorded speed from (DiurnalDic) 
	for the given roadway as a limit on predictions."""
	MaximumDic = {}
	for a in all_pair_ids.pair_id:
		print "Defining maximum travel times for roadway %d" % a
		processed_file_path = os.path.join(D['update_path'], "IndividualFiles", D['bt_name'] + "_" +
							str(a) + "_" + "Cleaned_Normalized_Weather.csv")
		if os.path.exists(processed_file_path):
			site_df = pd.read_csv(processed_file_path)
			max_time = np.max(site_df.speed)
			if math.isnan(max_time):
				MaximumDic[str(a)] = 0.001
			else:
				MaximumDic[str(a)] = max_time
		else:
			MaximumDic[str(a)] = 0.001 #the flag for a missing minimum time (avoids division by 0)
	with open(os.path.join(D['update_path'], 'MaximumDic.txt'), 'wb') as outfile:
		json.dump(MaximumDic, outfile)
	return MaximumDic


def GetZip(url, f_type):
	"""Download a file found at the (url) provided of (f_type) 'csv' or 'zip'."""
	f_type = '.' + f_type
	try: # Open the url
		f = urlopen(url)
		print "downloading " + url
		with open(os.path.join(D['bt_path'], "massdot_bluetoad_data" + f_type), "wb") as local_file: # Open our local file for writing
			local_file.write(f.read())
	#handle errors
	except HTTPError, e:
		print "HTTP Error:", e.code, url
	except URLError, e:
		print "URL Error:", e.reason, url
	return None

def Unzip(fname, out_path):
	"""Unzip the file provided (fname), and write to a file in the (out_path) directory."""
	fh = open(os.path.join(out_path,fname + ".zip"), 'rb')
	z = Z.ZipFile(fh)
	for name in z.namelist():
		print "Extracting %s" % name; z.extract(name, out_path) #extract the file
	fh.close()
	return None

def SubBt_Cleaned_to_PreNormalized(D, a):
	"""The cleaned bt file must be transformed from its original 'cleaned' format to a form with '/N' examples removed,
	strings converted to floats, and a day-of-week column added where needed.  Use the parameter dictionary (D) to convert roadway (a)"""
	sub_bt = pd.read_csv(os.path.join(D['update_path'], "IndividualFiles", D['bt_name'] + "_" + str(a) + "_Cleaned.csv"))
	if len(sub_bt) > 0: 
		sub_bt = data.CleanBlueToad(sub_bt, os.path.join(D['update_path'], "IndividualFiles"), D['bt_name'] + "_" + str(a)) #remove "/N" examples
		sub_bt = data.FloatConvert(sub_bt, os.path.join(D['update_path'], "IndividualFiles"), D['bt_name'] + "_" + str(a)) #convert strings to float where possible
		sub_bt = AddDayOfWeekColumn(sub_bt, os.path.join(D['update_path'], "IndividualFiles"), D['bt_name'] + "_" + str(a)) #0-Mon, 6-Sun
	else:
		sub_bt['time_of_day'], sub_bt['day_of_week'] = [], []
	return sub_bt

def HardCodedParameters():
	"""Returns a dictionary of parameters we are unlikely to change..."""
	D = {"bt_path" : os.path.join("scratch"),
	"update_path" : os.path.join("update"),
	"data_path" : os.path.join("data"),
	"bt_name" : "massdot_bluetoad_data",
	"pred_duration" : 288, #hours of prediction
	"weather_site_name" : "closest", "weather_site_default" : "BostonAirport",
	'w_def': 'Boston, Logan International Airport ',
	'steps_to_smooth': 12, #how long until our prediction fully reflects future estimates
	"window" : 12, #how many five-minute interval defines a suitable moving-average window
	"day_dict" : {'monday' : 0, 'tuesday' : 1, 'wednesday' : 2, 'thursday' : 3, 'friday' : 4, 
				  'saturday' : 5, 'sunday' : 6},
	#bt_proc can be "no_update" if we are not processing/normalizing...otherwise the whole process ensues
	"pct_range" : .1, #how far from the current traffic's percentile can we deem 'similar'?
	"time_range" : 10, #how far from the current time is considered 'similar'?
	"max_speed" : 85, #what is the highest speed will allow ourselves to report?
	"weather_fac_dic" : {' ': 1, 'RA' : 3, 'FG' : 5, 'SN' : 30}, #how many more must we grab, by cond?
	"pct_tile_list" : ['min', 10, 25, 50, 75, 90, 'max'], #which percentiles shall be made available,
										#along with the best and worst-case scenarios
	"path_to_lat_lons" : "https://github.com/apcollier/hacking-travel/blob/master/js/segments.js",
	"path_to_current" : "http://acollier.com/traffichackers/data/current.json", #traffichackers.com/data/predictions/similar_dow.json
	"CoordsDic_name" : "RoadwayCoordsDic.txt", "NOAA_df_name" : "WeatherSites_MA.csv",
	"WeatherInfo" : "ClosestWeatherSite.txt",
	"WeatherURL" : "http://w1.weather.gov/xml/current_obs/",
	"bluetoad_type" : "csv", #can be set to 'csv' or 'zip'
	"path_to_blue_toad_csv" :  "http://acollier.com/traffichackers/model_history.csv",
	"path_to_blue_toad_zip" : "https://raw.githubusercontent.com/hackreduce/MassDOThack/master/Road_RTTM_Volume/massdot_bluetoad_data.zip"}

	D["weather_dir"] = os.path.join(D['data_path'], "NCDC_Weather")
	return D

def PrePrep(D):
	"""Given the parameter dictionary (D), create directories where needed and download BlueToad data."""
	NOAA_df = pd.read_csv(os.path.join(D['data_path'], D['NOAA_df_name']))
	if not os.path.exists(os.path.join(D["update_path"])): os.makedirs(os.path.join(D["update_path"])) #add directories if missing
	if not os.path.exists(os.path.join(D["bt_path"])): os.makedirs(os.path.join(D["bt_path"]))
	if "zip" in D['bluetoad_type']: #if we are downloading a large .zip to unpack before running
		if not os.path.exists(os.path.join(D['bt_path'], D['bt_name'] + ".zip")): #download all data, then run a full update if it does not exist.
			if not os.path.exists(os.path.join(D['bt_path'], D['bt_name'] + ".csv")):
				url = D['path_to_blue_toad_speed_zip']
				GetZip(url, 'zip'); Unzip(D['bt_name'], D['bt_path']) #download the file, and unzip it.
	elif "csv" in D['bluetoad_type']: #if we are downloading a .csv before running
		if not os.path.exists(os.path.join(D['bt_path'], D['bt_name'] + ".csv")):
			url = D['path_to_blue_toad_speed_csv']
			GetZip(url, 'csv')
	if not os.path.exists(os.path.join(D["update_path"], "IndividualFiles")): os.makedirs(os.path.join(D["update_path"], "IndividualFiles"))
	return NOAA_df

def PredictionModule(all_pair_ids, pairs_and_conditions, D, subset, time_of_day,
					DiurnalDic, MaximumDic, day_of_week, current_datetime):
	"""Generate forward predictions, then unnormalize and return in dictionary form"""
	PredictionDic = GenerateNormalizedPredictions(all_pair_ids, pairs_and_conditions, D['weather_fac_dic'],
									day_of_week, current_datetime, D['pct_range'], D['time_range'],
									D['update_path'], D['bt_name'], D['pct_tile_list'], subset, 
									D['pred_duration'], time_of_day, D['weather_kernel_pct'], D['start_date'], D['end_date'])
	CurrentPredDic = UnNormalizePredictions(PredictionDic, DiurnalDic, MaximumDic, day_of_week, current_datetime, D['pred_duration'], 										time_of_day, D['max_speed'], pairs_and_conditions, D['steps_to_smooth'])
	return CurrentPredDic
	
def main(D, output_file_name, subset, time_of_day):
	"""Main module"""
	NOAA_df = PrePrep(D) #create directories and/or download bluetoad data if required.
	data.GetBlueToad(D, D['bt_name']) #read it in and re-format dates
	weights = list(pd.read_csv(os.path.join(D['data_path'],'DecaySeries.csv')).Weight)
	all_pair_ids = pd.read_csv(os.path.join(D['data_path'], "all_pair_ids.csv"))
	if not os.path.exists(os.path.join(D['update_path'], 'DiurnalDictionary.txt')): #if this dictionary doesn't exist, we'll fill it
		DiurnalDic = {} #To be appended, site by site
	else: #just read it it from file
		DiurnalDic = GetJSON(D['update_path'], "DiurnalDictionary.txt")
	DD_flag = False #assume we do NOT need to alter DiurnalDic...
	if os.path.exists(os.path.join(D['data_path'], D['CoordsDic_name'])):	#if we've already built it
		RoadwayCoordsDic = GetJSON(D['data_path'], D['CoordsDic_name'])
	else:
		RoadwayCoordsDic = mass.GetLatLons(D['data_path'], "Roadway_LatLonData.txt")
	if not os.path.exists(os.path.join(D['update_path'], D['WeatherInfo'])): #if we lack minimums for each site
		NOAADic = NCDC.BuildClosestNOAADic(NOAA_df, all_pair_ids.pair_id, D) #which weather site for which roadway?
	else:
		NOAADic = GetJSON(D['update_path'], D['WeatherInfo']) #read in the locations of closest weather sites
	DD_flag = False  	
	for a in all_pair_ids.pair_id: #process by site,
		flag = 0; ##Check if we've gone through the normalization steps...
		if not os.path.exists(os.path.join(D['update_path'], "IndividualFiles", D['bt_name'] + "_" + str(a) + "_Cleaned_Normalized.csv")):
			sub_bt = SubBt_Cleaned_to_PreNormalized(D, a); flag = 1 #to note that this process is already done.
		if str(a) + "_0" not in DiurnalDic.keys(): #if the DiurnalDictionary is still empty
			if not flag: #if we need to process of the sub_bt data frame again.
				sub_bt = SubBt_Cleaned_to_PreNormalized(D, a); flag = 1 #to note that this process is already done.
			DiurnalDic.update(GenerateDiurnalDic(sub_bt, D['update_path'], five_minute_fractions, D['window'])); DD_flag = True
		if not os.path.exists(os.path.join(D['update_path'], "IndividualFiles", D['bt_name'] + "_" + str(a) + "_Cleaned_Normalized.csv")):
			sub_bt = NormalizeTravelTime(sub_bt, DiurnalDic, os.path.join(D['update_path'], "IndividualFiles"), D['bt_name'] + "_" + str(a))
		if not os.path.exists(os.path.join(D['update_path'], "IndividualFiles", D['bt_name'] + "_" + str(a) + "_Cleaned_Normalized_Weather.csv")):
			sub_bt = pd.read_csv(os.path.join(D['update_path'], "IndividualFiles", D['bt_name'] + "_" + str(a) + "_Cleaned_Normalized.csv"))
			sub_bt = AttachWeatherData(sub_bt, os.path.join(D['update_path'], "IndividualFiles"), D['bt_name'] + "_" + str(a), D['weather_dir'], D["weather_site_default"])
		if not os.path.exists(os.path.join(D['update_path'], "IndividualFiles", D['bt_name'] + "_" + str(a) + "_CNW_TrafficHist.csv")):
			sub_bt = pd.read_csv(os.path.join(D['update_path'], "IndividualFiles", D['bt_name'] + "_" + str(a) + "_Cleaned_Normalized_Weather.csv"))		
			sub_bt = AttachTrafficHistory(sub_bt, os.path.join(D['update_path'], "IndividualFiles"), D['bt_name'] + "_" + str(a), D, weights)
		if not os.path.exists(os.path.join(D['update_path'], "IndividualFiles", D['bt_name'] + "_" + str(a) + "_CNW_TrafficHist_WeatherHist.csv")):
			sub_bt = pd.read_csv(os.path.join(D['update_path'], "IndividualFiles", D['bt_name'] + "_" + str(a) + "_CNW_TrafficHist.csv"))
			sub_bt = AttachWeatherHistory(sub_bt, os.path.join(D['update_path'], "IndividualFiles"), D['bt_name'] + "_" + str(a), D, weights)	
	#Write full DiurnalDictionary to a .txt file as a .json
	if DD_flag:
		with open(os.path.join(D['update_path'], 'DiurnalDictionary.txt'), 'wb') as outfile:
			json.dump(DiurnalDic, outfile)
	if not os.path.exists(os.path.join(D['update_path'], 'MaximumDic.txt')): #if we lack minimums for each site
		MaximumDic = DefineMaximums(D, all_pair_ids)
	else:
		MaximumDic = GetJSON(D['update_path'], "MaximumDic.txt") #read in the minimum predictions
	if D['predict'] != 0: #if we are generating forward predictions 
		day_of_week, current_datetime, pairs_and_conditions = mass.GetCurrentInfo(D['path_to_speed_history'], DiurnalDic, D['traffic_system_memory'], weights, D['path_to_current'])
		if time_of_day == "": #if we are interested in predictions based on current conditions
			pairs_and_conditions = NCDC.RealTimeWeather(D, NOAADic, NOAA_df, pairs_and_conditions, weights)
		else: #zero-out the normalized conditions, historical analysis starts from a normalized baseline of zero (typical conditions)
			if subset[-1] in ['0','1','2','3','4','5','6']: #if there is a prescribed day_of_week...
				day_of_week = int(subset[-1]) #force day_of_week to chosen day rather than current day
			elif 'S' in subset:
				day_of_week = max(5, day_of_week) #to ensure appropriate UnNormalization (Sat - Sun as weekend standard)
			elif 'Y' in subset:
				day_of_week = 1 if day_of_week > 4 else day_of_week #to ensure appropriate UnNormalization (Tue - Thu as weekday standard)
			for k in pairs_and_conditions.keys():
				pairs_and_conditions[k][0] = 0
		if 'O' in subset: subset += str(day_of_week) #this means we are running the model based on whatever 'today' is.
		CurrentPredDic = PredictionModule(all_pair_ids, pairs_and_conditions, D, subset, time_of_day,
							DiurnalDic, MaximumDic, day_of_week, current_datetime)
	else: #no need to spend time on gathering similar sets and unnormalizing
		CurrentPredDic = NoPrediction(all_pair_ids, D) #if we are simply reporting a JSON for the relevant time subset
	with open(os.path.join(D['update_path'], output_file_name), 'wb') as outfile:
		json.dump(CurrentPredDic, outfile)
	return None
	
if __name__ == "__main__":
	#'W' - weather, 'T' - traffic conditions, 'D' - day of week, 'S' - Sat/Sun vs. Mon-Fri.  The options
	#are invoked by including the letters in the input string, subset.  For example.  Using 'TD' as the
	#string will choose examples based on traffic and the day of week, but not weather...
	D = {"pred_duration" : 288, #hours of prediction
	'steps_to_smooth': 12, #how long until our prediction fully reflects future estimates
	"window" : 12, #how many five-minute interval defines a suitable moving-average window
	"day_dict" : {'monday' : 0, 'tuesday' : 1, 'wednesday' : 2, 'thursday' : 3, 'friday' : 4, 
				  'saturday' : 5, 'sunday' : 6},
	#bt_proc can be "no_update" if we are not processing/normalizing...otherwise the whole process ensues
	"pct_range" : .1, #how far from the current traffic's percentile can we deem 'similar'?
	"time_range" : 10, #how far from the current time is considered 'similar'?
	"max_speed" : 85, #what is the highest speed will allow ourselves to report?
	"weather_fac_dic" : {' ': 1, 'RA' : 3, 'FG' : 10, 'SN' : 30}, #how many more must we grab, by cond?
	"pct_tile_list" : ['min', 10, 25, 50, 75, 90, 'max'], #which percentiles shall be made available,
										#along with the best and worst-case scenarios
	"traffic_system_memory" : 72, #number of 5-min-steps to consider for weather conditions/traffic conditions
	"traffic_similarity_pct" : 0.2, #how similar must historical traffic be? (0.1 means we located the 10% most similar)
	"weather_kernel_pct" : 0.3, #how similar must historical weather be?
	"weather_cost_facs" : {"SN" : 3, "RA" : 1, "FG" : 1, " " : 0}
	}
	environment_vars = GetJSON("","config.json")
	for key in environment_vars:
		D[key] = environment_vars[key] #add environmental variables to the larger dictionary
	D["weather_dir"] = os.path.join(D['data_path'], "NCDC_Weather")

	parser = argparse.ArgumentParser()
	parser.add_argument("day", choices=['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
						'today', 'weekday', 'weekend'], help = "day of week option, must be a lowercase day of the week, 'today', 'weekday', or 'weekend'")						
	parser.add_argument("output_file_name", help = "the name of the file ('.txt' included) to which predictions are written.")
	parser.add_argument("-t", "--traffic", help = "traffic, can be included as '-t' or '-traffic'", action = "count")
	parser.add_argument("-w", "--weather", help = "weather, can be included as '-w' or '--weather'", action = "count")
	parser.add_argument("-l", "--length", help = "user-specified duration, in five-minute increments, default of 288 (one day)", 
						type = int, default = 288) 
	parser.add_argument("-hr", "--hour", help = "choose an hour of the day, of the form 00:00 - 23:59.  It will round to the nearest 5min.  It negates the usage of weather or traffic.", type = str, default = '')
	parser.add_argument("-sd", "--start_date", help = "the start date to be considered from the historical data (YYYYDOY format).",
						type = int, default = 0)
	parser.add_argument("-ed", "--end_date", help = "the end date to be considered from the historical data (YYYYDOY format).",
						type = int, default = 9999999)
	parser.add_argument("-p", "--predict", help = "set to any value other than 0 to make predictions rather than report the data itself.",
						type = int, default = 1)
	args = parser.parse_args()

	subset = '' #to be added based on user provided arguments:
	if args.weather >= 1 and args.hour == '': subset += 'W'  #only incorporate weather/traffic if this is not a historical...
	if args.traffic >= 1 and args.hour == '': subset += 'T'  #...specific time-of-day analysis
	D['pred_duration'] = args.length #define the length of prediction
	
	#define the day of week scope to be considered
	if args.day == 'weekend': 
		subset += 'S'; print "WEEKEND ANALYSIS"
	elif args.day == 'weekday': 
		subset += 'Y'; print "WEEKDAY ANALYSIS"
	elif args.day == 'today':
		subset += 'O'; print "ANAYLSIS FOR TODAY"
	else:
		subset += str(D['day_dict'][args.day]); 
	out_name = args.output_file_name
	
	#define the temporal span to be considered
	D['start_date'] = args.start_date; D['end_date'] = args.end_date
	
	#define whether predictive analytics are necessary
	D['predict'] = args.predict
	
	if args.hour != '' and ":" in args.hour and len(args.hour) == 5: #if we are looking for a specific day/time pairing historically rather than a prediction based on current conditions
		hour, minute = args.hour.split(":")
		time_of_day = float(hour)/24 + float(minute)/24/60
		if time_of_day >= 1: time_of_day = time_of_day - int(time_of_day) #in case the number input exceeds 23:59.
		time_of_day = NCDC.RoundToNearestNth(time_of_day, 288, 3) #return a five minute fraction (0/288 to 277/288)
	else: 
		time_of_day = ""
	print out_name, subset, D['pred_duration'], time_of_day
	main(D, out_name, subset, time_of_day)
	
	
