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
	previous_id = 999999
	for row in xrange(len(bt)): #zipping takes up too much RAM...requires an iterator
		if bt.pair_id[row] != previous_id:
			previous_id = bt.pair_id[row]
			print "Now normalizing site %d" % bt.pair_id[row]
		diurnal_key = str(bt.pair_id[row]) + "_" + str(bt.day_of_week[row]) #to find the correct key of the dictionary
		time_index = int((bt.insert_time[row] - int(bt.insert_time[row])) * 288 + .0001)
		normalized_times.append(bt.travel_time[row] - DiurnalDic[diurnal_key][time_index])
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
	weather_data = ProcessWeatherData(w_dir, w_site_name)
	bt = AppendWeatherInformation(weather_data, bt)
	bt.to_csv(os.path.join(blue_toad_path, blue_toad_name + "_Cleaned" + "_Normalized" + "_Weather.csv"), index = False)
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
	
def GenerateNormalizedPredictions(bt, ps_and_cs, weather_fac_dic, 
								day_of_week, pct_range, time_range):
	"""Iterate over all pair_ids and determine similar matches in terms of time_of_day,
	weather, traffic, and day_of_week...and generate 288 five-minute predictions (a
	24-hour prediction in 5-minute intervals)"""
	all_pair_ids = mass.unique(bt.pair_id) #obtain a list of unique pair_ids		
	PredictionDic = {}
	for a in all_pair_ids: #iterate over each pair_id and generate a string of predictions 
		sub_bt = bt[bt.pair_id == a] #just the given stretch of road
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
		pred_list = [] #will predict 5 min, 10 min, ... , 23hrs and 55min, 24 hrs
		print "Generating Predictions for site %d" % a
		for ind,f in enumerate(five_minute_fractions): #for generating of predictions at each forward step
			viable_future_indices = [day_sub_bt.index[i] + ind + 1 for i in xrange(len(day_sub_bt)) if i + ind + 1 < L]
			prediction_sub = sub_bt[sub_bt.index.isin(viable_future_indices)]
			pred_list.append(np.average(prediction_sub.Normalized_t))
		PredictionDic[str(a)] = pred_list #append the 288-element string of predictions	
	with open(os.path.join(blue_toad_path, 'CurrentPredictions.txt'), 'w') as outfile:
		json.dump(DiurnalDic, outfile)
	return PredictionDic

def UnNormalizePredictions(PredictionDic, DiurnalDic, day_of_week):
	"""Turn the normalized predictions from (PredictionDic) back into the standard-form
	estimates by using (DiurnalDic)."""
	UnNormDic = {}
	for key in PredictionDic.keys():
		std_seq = DiurnalDic[key + "_" + str(day_of_week)]
		norm_seq = PredictionDic[key]
		UnNormDic[key] = [s + n for s,n in zip(std_seq, norm_seq)]
	return UnNormDic
	
def GetJSON(f_path, f_name):
	"""Read in a JSON of (f_name) at (f_path)."""
	json_data = open(os.path.join(f_path, f_name)).read()
	return json.loads(json_data)	

def HardCodedParameters():
	"""This might later be converted to a dictionary for convenience.  As it stands,
	this returns a number of parameters we are unlikely to change..."""
	blue_toad_path = os.path.join("..","..","..","Boston_Andrew","MassDothack-master", "Road_RTTM_Volume")
	blue_toad_name = "massdot_bluetoad_data.csv"
	bt_proc = "W"; weather_site_name = "BostonAirport"
	window = 12 #how many five-minute interval defines a suitable moving-average window
	#bt_proc can be "W" for weather-added, "N" for 'normalized', "C" for 'cleaned' 
	#or anything else to denote uncleaned.
	weather_dir = os.path.join(blue_toad_path, "..", "..", "NCDC_Weather")
	pct_range = .1 #how far from the current traffic's percentile can we deem 'similar'?
	time_range = 10 #how far from the current time is considere 'similar'?
	weather_fac_dic = {' ': 1, 'RA' : 3, 'FG' : 5, 'SN' : 10} #how many more must we grab, by cond?		
	return blue_toad_path, blue_toad_name, bt_proc, weather_site_name, window, weather_dir, pct_range, time_range, weather_fac_dic
	
if __name__ == "__main__":
	#script_name, blue_toad_path, blue_toad_name, bt_proc, weather_site_name = sys.argv
	blue_toad_path, blue_toad_name, bt_proc, weather_site_name, window, weather_dir, pct_range, time_range, weather_fac_dic = HardCodedParameters()
	if "W" in bt_proc: #cleaned, normalized, and weather already attached (open only 1M rows for memory)
		bt = pd.read_csv(os.path.join(blue_toad_path, blue_toad_name + "_Cleaned" + "_Normalized" + "_Weather.csv"), nrows = 1000000)
		DiurnalDic = GetJSON(blue_toad_path, "DiurnalDictionary.txt")
	elif "N" in bt_proc: #cleaned and normalized already
		bt = pd.read_csv(os.path.join(blue_toad_path, blue_toad_name + "_Cleaned" + "_Normalized.csv"))
		bt = AttachWeatherData(bt, blue_toad_path, blue_toad_name, weather_dir, weather_site_name)
		DiurnalDic = GetJSON(blue_toad_path, "DiurnalDictionary.txt")
	elif "C" in bt_proc: #this is cleaned, but not yet normalized
		bt = data.GetBlueToad(blue_toad_path, True, blue_toad_name)
		if os.path.exists(os.path.join(blue_toad_path, "DiurnalDictionary.txt")): #if we've already generated our diurnal dict.
			DiurnalDic = GetJSON(blue_toad_path, "DiurnalDictionary.txt")
		else:
			DiurnalDic = GenerateDiurnalDic(bt, blue_toad_path, five_minute_fractions, window)
		bt = NormalizeTravelTime(bt, DiurnalDic, blue_toad_path, blue_toad_name)
	else: #if we need to process everything
		bt = data.GetBlueToad(blue_toad_path, False, blue_toad_name) #read it in and re-format dates
		bt = data.CleanBlueToad(bt, blue_toad_path, blue_toad_name) #remove "/N" examples
		bt = data.FloatConvert(bt, blue_toad_path, blue_toad_name) #convert strings to float where possible
		bt = AddDayOfWeekColumn(bt, blue_toad_path, blue_toad_name) #0-Mon, 6-Sun
		DiurnalDic = GenerateDiurnalDic(bt, blue_toad_path, five_minute_fractions, window)
		bt = NormalizeTravelTime(bt, DiurnalDic, blue_toad_path, blue_toad_name)
		bt = AttachWeatherData(bt, blue_toad_path, blue_toad_name, weather_dir, weather_site_name)

	day_of_week, pairs_and_conditions = mass.GetCurrentInfo('http://www.acollier.com/massdot/current.json',
												DiurnalDic)
	PredictionDic = GenerateNormalizedPredictions(bt, pairs_and_conditions, weather_fac_dic, 
												day_of_week, pct_range, time_range)
	CurrentPredDic = UnNormalizePredictions(PredictionDic, DiurnalDic, day_of_week)
	with open(os.path.join(blue_toad_path, 'CurrentPredictions.txt'), 'w') as outfile:
		json.dump(CurrentPredDic, outfile)

	