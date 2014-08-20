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
	
def DefineDiurnalCycle(sub_bt, day, five_minute_fractions, MA_smooth_fac):
	"""Given a specific pair_id and day-of-the-week, return a diurnal cycle list, per five-minute
	interval over the the 288 5-minute intervals of the day.  (MA_smooth_fac) defines the number
	of five-minute intervals used to general the moving average.  Ex: 12 implies a one-hour total 
	window (30 minutes on either side)"""
	diurnal_cycle = []
	times_of_day = [round(i - int(i),3) for i in sub_bt.insert_time] #just the fraction of the day
	sub_bt['times_of_day'] = times_of_day
	for f in five_minute_fractions:
		sub_cycle = sub_bt[sub_bt.times_of_day == round(f,3)]
		if len(sub_cycle) == 0: #meaning there are no examples on this day of the week at this time
			diurnal_cycle.append(diurnal_cycle[-1]) #assume the previous five-minute timestamp's time
		else:
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
	
def GetCorrectDaytimes(traffic_sub_bt, day_of_week, current_time, subset):
	"""Given the similar traffic examples in (traffic_sub_bt), the (day_of_week), the (current_time), and
	descriptions in (subset) of whether we are interested in the same day or simply weekday/weekend matches, 
	return the list of daytimes that are appropriate."""
	if 'D' in subset: #if we're working our way down to a specific day-of-the-week
		return traffic_sub_bt[np.logical_and(traffic_sub_bt.day_of_week == day_of_week, 
									  traffic_sub_bt.time_of_day == current_time)], [day_of_week]
		
	else: #'S' in subset, grab weekday or weekends
		if day_of_week >= 5: #it's a weekend
			return traffic_sub_bt[np.logical_and(traffic_sub_bt.day_of_week >= 5, 
									  raffic_sub_bt.time_of_day == current_time)], [5,6]
		else: #it's a weekday	
			return traffic_sub_bt[np.logical_and(traffic_sub_bt.day_of_week < 5, 
									  traffic_sub_bt.time_of_day == current_time)], [0,1,2,3,4]
									  
def GetSub_Times_and_Days(traffic_sub_bt, current_datetime, subset, time_range, day_of_week):
	"""Return a dataframe which only contains entries with the same time as the present
	or times within (time_range) of the current time on the appropriate days of the week. 
	For instance, 11:00pm times could include 12:30am as 'similar' examples.  These would be on the 
	following day.  If 'S' in (subset), we will return days 0-4, weekdays, or 5-6, weekends."""
	current_time = NCDC.GetTimeFromDateTime(current_datetime) #0-1, three decimal time of day (.875, e.g.)
	current_day = current_datetime.day #day of the month (allows distinguishing similar times on other days)
	correct_daytimes, viable_days = GetCorrectDaytimes(traffic_sub_bt, day_of_week, current_time, subset)
			
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
				correct_daytimes = correct_daytimes.append(traffic_sub_bt[np.logical_and(traffic_sub_bt.day_of_week == new_day_of_week, 
										  traffic_sub_bt.time_of_day == new_time)])
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
	
def GenerateNormalizedPredictions(all_pair_ids, ps_and_cs, weather_fac_dic, day_of_week, 
								  current_datetime, pct_range, time_range, bt_path, bt_name, pcts, subset):
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
			if 'W' in subset:
				weather_sub_bt = sub_bt[sub_bt.weather == ps_and_cs[str(a)][1]] #just similar weather
			else:
				weather_sub_bt = sub_bt
			L_w = len(weather_sub_bt)
			if 'T' in subset:
				traffic_sub_bt = GetSub_Traffic(weather_sub_bt, ps_and_cs[str(a)][0], pct_range, L_w)
			else:
				traffic_sub_bt = weather_sub_bt
			traffic_sub_bt['time_of_day'] = [round(traffic_sub_bt.insert_time[i] - int(traffic_sub_bt.insert_time[i]),3) 
											for i in traffic_sub_bt.index]
			#locate similar days/times, more lax search in less common weather	
			if 'D' in subset or 'S' in subset: #if we need to choose only certain days of the week
				day_sub_bt	= GetSub_Times_and_Days(traffic_sub_bt, current_datetime, subset,
								time_range * weather_fac_dic[ps_and_cs[str(a)][1]], day_of_week)
				while len(day_sub_bt) < 5: #if our similarity requirements are too stringent
					time_range = time_range * 1.5
					day_sub_bt	= GetSub_Times_and_Days(traffic_sub_bt, current_datetime, subset,
							  time_range * weather_fac_dic[ps_and_cs[str(a)][1]], day_of_week)
			else:
				day_sub_bt = traffic_sub_bt
			for p in pcts:
				PredictionDic[str(a)][str(p)] = [] #will predict 5 min, 10 min, ... , 23hrs and 55min, 24 hrs
							  #######Generate Predictions#####
			print "Generating Predictions for site %d" % a
			for ind,f in enumerate(five_minute_fractions): #for generating of predictions at each forward step
				viable_future_indices = [day_sub_bt.index[i] + ind + 1 for i in xrange(len(day_sub_bt)) if i + ind + 1 < L]
				prediction_sub = sub_bt[sub_bt.index.isin(viable_future_indices)]
				for p in pcts: #iterate over the percentiles required for estimation
					if p == 'min': #if we're estimating a best case
						PredictionDic[str(a)][str(p)].append(np.min(prediction_sub.Normalized_t))	
					elif p == 'max': #if we're estimating a worst case 
						PredictionDic[str(a)][str(p)].append(np.max(prediction_sub.Normalized_t))
					else:
						PredictionDic[str(a)][str(p)].append(np.percentile(prediction_sub.Normalized_t, p))	
		else: #use default...essentially dead-average conditions, flagged as -0.00001 rather than zero
			print "No current information available for site %d, using default." % a
			pred_list = [-0.00001 for i in range(288)]
			for p in pcts: #NOTE, WITHOUT CURRENT INFO, ALL PERCENTILES WILL BE THE SAME (DEFAULT)
				PredictionDic[str(a)][str(p)] = pred_list
	#with open(os.path.join(bt_path, 'CurrentPredictions.txt'), 'w') as outfile:
	#	json.dump(PredictionDic, outfile)
	return PredictionDic

def UnNormalizePredictions(PredictionDic, DiurnalDic, MinimumDic, day_of_week, current_datetime):
	"""Turn the normalized predictions from (PredictionDic) back into the standard-form
	estimates by using (DiurnalDic)."""
	UnNormDic = {}
	UnNormDic['Start'] = current_datetime.isoformat() #current time, each prediction is 5,10,...minutes after
	for road in PredictionDic.keys(): #iterate over all pair_ids
		min_time = MinimumDic[road] #shortest historical travel time for a roadway
		UnNormDic[str(road)] = {}
		std_seq = GetStandardSequence(road, day_of_week, current_datetime, DiurnalDic)
		for p in PredictionDic[str(road)].keys():
			norm_seq = PredictionDic[str(road)][str(p)]
			UnNormDic[str(road)][str(p)] = [max(s + n , min_time) for s,n in zip(std_seq, norm_seq)]
	return UnNormDic

def GetTimeVec(current_datetime, ntimes):
	""" Given a (current_datetime) of datetime format, fill a vector with 288 times of the form: 
	"21:05,16".  In this case, "16" will represent the day of the month, which will shift
	when the prediction crosses over into the following day.  At the end of the month, strings will
	change from '23:55,31' to '00:00,1', e.g."""
	five_min_steps = [5 + 5*i for i in range(ntimes)] #5, 10, 15, ..., 8640
	time_vec = [] 
	for f in five_min_steps:	
		new_date = current_datetime + datetime.timedelta(minutes = f)
		time_vec.append(NDigitString(2,new_date.hour) + ":" + NDigitString(2,new_date.minute) + "," +
						str(new_date.day))
	return time_vec	
	
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
	
def GetStandardSequence(road, day_of_week, current_datetime, DiurnalDic):
	"""Given the (road), (day_of_week), the (current_datetime) at which we are looking to make predictions
	forward in time, and the (DiurnalDic) used for baseline expectations, return the 288 next baseline
	travel times.  Note, if we are beginning at 12pm on Sunday, within 24 hours, we will be making
	predictions for the subsequent Monday morning, a different list from DiurnalDic"""
	current_Diurnal_key = str(road) + "_" + str(day_of_week)
	new_datetime = current_datetime + datetime.timedelta(minutes = 1440) #the 'next' day
	new_day_of_week = AdjustDayOfWeek(current_datetime.day, new_datetime.day, day_of_week)
	next_Diurnal_key = str(road) + "_" + str(new_day_of_week) #the DiurnalDic key for next-day predictions
	day_index = GetIndexFromDatetime(current_datetime) #how many 5-minute intervals are we into the day
	current_seq, next_seq = DiurnalDic[current_Diurnal_key][day_index+1:], DiurnalDic[next_Diurnal_key][0:day_index+1]
	return current_seq + next_seq
	
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
	
def DefineMinimums(D, all_pair_ids):
	"""To avoid predictions of unrealistically high travel speeds, given a dictionary (D) of parameters,
	and a list of (all_pair_ids), return the minimum recorded travel time for the given roadway as a 
	limit on predictions."""
	MinimumDic = {}
	for a in all_pair_ids.pair_id:
		print "Definining minimum travel times for roadway %d" % a
		processed_file_path = os.path.join(D['update_path'], "IndividualFiles", D['bt_name'] + "_" +
							str(a) + "_" + "Cleaned_Normalized_Weather.csv")
		if os.path.exists(processed_file_path):
			site_df = pd.read_csv(processed_file_path)
			MinimumDic[str(a)] = np.min(site_df.travel_time)
		else:
			MinimumDic[str(a)] = 0.001 #the flag for a missing minimum time (avoids division by 0)
	with open(os.path.join(D['update_path'], 'MinimumPredictions.txt'), 'w') as outfile:
		json.dump(MinimumDic, outfile)
	return MinimumDic

def GetZip(url):
	"""Download a .zip file found at the (url) provided."""
	try: # Open the url
		f = urlopen(url)
		print "downloading " + url
		with open(os.path.join(D['bt_path'], "massdot_bluetoad_data.zip"), "wb") as local_file: # Open our local file for writing
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

def HardCodedParameters():
	"""Returns a dictionary of parameters we are unlikely to change..."""
	D = {"bt_path" : os.path.join("scratch"),
	"update_path" : os.path.join("update"),
	"data_path" : os.path.join("data"),
	"bt_name" : "massdot_bluetoad_data",
	"weather_site_name" : "closest", "weather_site_default" : "BostonAirport", 
	'w_def': 'Boston, Logan International Airport ',
	"window" : 12, #how many five-minute interval defines a suitable moving-average window
	#bt_proc can be "no_update" if we are not processing/normalizing...otherwise the whole process ensues
	"pct_range" : .1, #how far from the current traffic's percentile can we deem 'similar'?
	"time_range" : 10, #how far from the current time is considered 'similar'?
	"weather_fac_dic" : {' ': 1, 'RA' : 3, 'FG' : 5, 'SN' : 10}, #how many more must we grab, by cond?
	"pct_tile_list" : ['min', 10, 20, 25, 30, 50, 70, 75, 90, 'max'], #which percentiles shall be made available, 
										#along with the best and worst-case scenarios
	"path_to_lat_lons" : "https://github.com/apcollier/hacking-travel/blob/master/js/segments.js",
	"path_to_current" : "http://traffichackers.com/current.json",
	"CoordsDic_name" : "RoadwayCoordsDic.txt", "NOAA_df_name" : "WeatherSites_MA.csv",
	"WeatherInfo" : "ClosestWeatherSite.txt",
	"WeatherURL" : "http://w1.weather.gov/xml/current_obs/",
	"path_to_blue_toad" : "https://raw.githubusercontent.com/hackreduce/MassDOThack/master/Road_RTTM_Volume/massdot_bluetoad_data.zip"}	
												
	D["weather_dir"] = os.path.join(D['bt_path'], "NCDC_Weather")
	return D
	
if __name__ == "__main__":
	script_name, bt_proc, subset = sys.argv #for the subset variable the following options are available:
	#'W' - weather, 'T' - traffic conditions, 'D' - day of week, 'S' - Sat/Sun vs. Mon-Fri.  The options
	#are invoked by including the letters in the input string, subset.  For example.  Using 'TD' as the
	#string will choose examples based on traffic and the day of week, but not weather...
	D = HardCodedParameters()
	NOAA_df = pd.read_csv(os.path.join(D['data_path'], D['NOAA_df_name']))
	if bt_proc in ['scratch', 'Scratch', 'SCRATCH', 's', 'S']: #download all data, then run a full update.
		url = D['path_to_blue_toad']
		GetZip(url); Unzip(D['bt_name'], D['bt_path']) #download the file, and unzip it.
		bt_proc = 'U' #to ensure we'll run the update.
	if bt_proc in ['update', 'Update', 'UPDATE', 'u', 'U']: #run the full update
		all_pair_ids = pd.read_csv(os.path.join(D['data_path'], "all_pair_ids.csv"))
		#all_pair_ids must exist.  The file can be shortened to only include certain roadways.
		DiurnalDic = GetJSON(D['update_path'], "DiurnalDictionary.txt") #on 'no update', we just read-in the Dic
		MinimumDic = GetJSON(D['update_path'], "MinimumPredictions.txt") #read in the minimum predictions
		NOAADic = GetJSON(D['update_path'], D['WeatherInfo']) #read in the locations of closest weather sites
		if os.path.exists(os.path.join(D['update_path'], D['CoordsDic_name'])):	#if we've already built it
			RoadwayCoordsDic = GetJSON(D['update_path'], D['CoordsDic_name'])
		else:
			RoadwayCoordsDic = mass.GetLatLons(D['data_path'], "Roadway_LatLonData.txt")
			
		for a in all_pair_ids.pair_id: #site-by-site, adding what is needed for each
			print "Processing roadway %d" % a 
			if not os.path.exists(os.path.join(D['update_path'], "IndividualFiles", D['bt_name'] + "_" +
							str(a) + "_" + "Cleaned_Normalized.csv")): #normalization and weather still needed.
				sub_bt = pd.read_csv(os.path.join(D['update_path'], "IndividualFiles", D['bt_name'] + "_" + str(a) + "_Cleaned.csv"))
				sub_bt = NormalizeTravelTime(sub_bt, DiurnalDic, os.path.join(D['update_path'], "IndividualFiles"), 
										D['bt_name'] + "_" + str(a))
			if not os.path.exists(os.path.join(D['update_path'], "IndividualFiles", D['bt_name'] + "_" +
							str(a) + "_" + "Cleaned_Normalized_Weather.csv")): #weather needed
				weather_site_name = NCDC.GetWSiteName(D, a, RoadwayCoordsDic) #which weather site is relevant?
				sub_bt = pd.read_csv(os.path.join(D['update_path'], "IndividualFiles", 
						D['bt_name'] + "_" + str(a) + "_Cleaned_Normalized.csv"))
				sub_bt = AttachWeatherData(sub_bt, os.path.join(D['update_path'], "IndividualFiles"), 
						D['bt_name'] + "_" + str(a), D['weather_dir'], weather_site_name)					
			else:
				pass

	elif bt_proc in ['no update', 'no_update', 'No_Update', 'n', 'N']: #if we need to process everything
		data.GetBlueToad(D['bt_path'], D['bt_name']) #read it in and re-format dates
		all_pair_ids = pd.read_csv(os.path.join(D['update_path'], "all_pair_ids.csv"))
		DiurnalDic = {} #To be appended, site by site
		MinimumDic = DefineMinimums(D, all_pair_ids)
		NOAADic = NCDC.BuildClosestNOAADic(NOAA_df, all_pair_ids.pair_id, D) #which weather site for which roadway?
		RoadwayCoordsDic = mass.GetLatLons(D['data_path'], "Roadway_LatLonData.txt")
		for a in all_pair_ids.pair_id: #process by site, 
			weather_site_name = NCDC.GetWSiteName(D, a, RoadwayCoordsDic) #which weather site is relevant?
			sub_bt = pd.read_csv(os.path.join(D['update_path'], "IndividualFiles", 
											  D['bt_name'] + "_" + str(a) + "_Cleaned.csv"))
			sub_bt = data.CleanBlueToad(sub_bt, os.path.join(D['update_path'], "IndividualFiles"), 
										D['bt_name'] + "_" + str(a)) #remove "/N" examples
			sub_bt = data.FloatConvert(sub_bt, os.path.join(D['update_path'], "IndividualFiles"), 
										D['bt_name'] + "_" + str(a)) #convert strings to float where possible
			sub_bt = AddDayOfWeekColumn(sub_bt, os.path.join(D['update_path'], "IndividualFiles"), 
										D['bt_name'] + "_" + str(a)) #0-Mon, 6-Sun
			DiurnalDic.update(GenerateDiurnalDic(sub_bt, D['update_path'], five_minute_fractions, D['window']))
			sub_bt = NormalizeTravelTime(sub_bt, DiurnalDic, os.path.join(D['update_path'], "IndividualFiles"), 
										D['bt_name'] + "_" + str(a))
			##########weather_site_name = GetWeatherSite(lat, lon) #to determine where to fetch weather conditions							
			sub_bt = AttachWeatherData(sub_bt, os.path.join(D['update_path'], "IndividualFiles"), 
										D['bt_name'] + "_" + str(a), D['weather_dir'], weather_site_name)
			#Write full DiurnalDictionary to a .txt file as a .json
		with open(os.path.join(D['update_path'], 'DiurnalDictionary.txt'), 'w') as outfile:
			json.dump(DiurnalDic, outfile)
	day_of_week, current_datetime, pairs_and_conditions = mass.GetCurrentInfo(D['path_to_current'], DiurnalDic)
	pairs_and_conditions = NCDC.RealTimeWeather(D, NOAADic, NOAA_df, pairs_and_conditions)	
	PredictionDic = GenerateNormalizedPredictions(all_pair_ids, pairs_and_conditions, D['weather_fac_dic'], 
									day_of_week, current_datetime, D['pct_range'], D['time_range'], 
									D['update_path'], D['bt_name'], D['pct_tile_list'], subset)
	CurrentPredDic = UnNormalizePredictions(PredictionDic, DiurnalDic, MinimumDic, day_of_week, current_datetime)
	with open(os.path.join(D['update_path'], 'CurrentPredictions.txt'), 'w') as outfile:
		json.dump(CurrentPredDic, outfile)

		
	
