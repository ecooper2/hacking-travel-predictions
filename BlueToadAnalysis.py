"""This module will generate predictions for the pair_ids contained in the BlueToad data"""

import ParseRealTimeMassDot as mass
import os
import pandas as pd
import numpy as np
import MassDotDataTypes as data
import datetime
import json
import NCDC_WeatherProcessor as NCDC

def AddDayOfWeekColumn(blue_toad, blue_toad_path):
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
	blue_toad.to_csv(os.path.join("MassDOThack-master","Road_RTTM_Volume",blue_toad_path[:(len(blue_toad_path)-4)]) + "_Cleaned.csv",
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
	
def GenerateDiurnalDic(bt, five_minute_fractions, window = 12):
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
	with open('DiurnalDictionary.txt', 'w') as outfile:
		json.dump(DiurnalDic, outfile)
	return DiurnalDic

def NormalizeTravelTime(bt, DiurnalDic):
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
	bt.to_csv(os.path.join("MassDOThack-master","Road_RTTM_Volume",blue_toad_path[:(len(blue_toad_path)-4)]) + "_Normalized.csv",
						index = False)
	return bt

def AppendWeatherInformation(weather_data, sub_bt):
	"""For a given (sub_bt), generally subset by the relevant pair_id, append the appropriate
	weather classification from the NOAA (weather_data)"""
	sub_bt.weather = [weather_data.WeatherType[weather_data.bt_date == sub_bt
	return weather_data
	
def SimplifyWeatherData(weather_dir, site_name):
	"""For a given NOAA (site_name) within the (weather_dir), return the appropriately subset
	weather data."""
	weather_data = pd.read_csv(os.path.join(weather_dir, site_name + "_NCDC.csv"))
	weather_data = weather_data.loc[:,("Date", "Time", "WeatherType")]	
	weather_data['bt_date'] = [ConvertWeatherDate(d, t, 24, 3) for d,t in 
							zip(weather_data.Date, weather_data.Time)]
	return weather_data

if __name__ == "__main__":
	script_name, blue_toad_path, bt_Cleaned, weather_dir = sys.argv
	##ex 'massdot_bluetoad_data_Cleaned.csv', T
	if bt_Cleaned == "T" or bt_Cleaned == "t" or bt_Cleaned == "True" or bt_Cleaned == "TRUE":
		bt = data.GetBlueToad(blue_toad_path, True)
	else:
		bt = data.GetBlueToad(blue_toad_path, False)
		bt = data.CleanBlueToad(bt, blue_toad_path)
		bt = data.FloatConvert(bt, blue_toad_path)
		bt = AddDayOfWeekColumn(bt, blue_toad_path)
	if os.path.exists("DiurnalDictionary.txt"): #if we've already generated our diurnal dict.
		json_data = open('DiurnalDictionary.txt').read()
		DiurnalDic = json.loads(json_data)
	else:
		window = 12 #how many five-minute interval defines a suitable moving-average window
		DiurnalDic = GenerateDiurnalDic(bt, five_minute_fractions, window)
	if not "Normalized_t" in bt.columns: #if the data are not normalized
		bt = NormalizeTravelTime(bt, DiurnalDic)