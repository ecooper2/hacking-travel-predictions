"""This module should read all relevant NCDC weather files for a given site (user-specified), 
traversing the directory for those files and returning a single data-frame containing all 
relevant months."""

import sys
import os
import pandas as pd
import numpy as np
import BlueToadAnalysis as BTA

global days_in_months 
global leaps
days_in_months = np.cumsum([31,28,31,30,31,30,31,31,30,31,30,31]) #for date conversion
leaps = [1900 + 4 * x for x in range(50)] #for leap year determination

def GetTimeFromDateTime(now, time = True, d_i_m = days_in_months, ls = leaps):
	"""Given a (now) from datetime.datetime.now, return the standard YYYYDOY.XXX 
	in five-minute fractions..."""
	w_date = now.year * 10000 + now.month * 100 + now.day
	w_time = now.hour * 100 + now.minute + float(now.second)/60
	now_time = ConvertWeatherDate(w_date, w_time, 288, 3, d_i_m = days_in_months, ls = leaps)
	if time: #if we're only returning the fraction of the day (.XXX)
		return round(now_time - int(now_time),3)
	else: #if we want the (YYYYDOY)
		return int(now_time)
		
def GetRelevantFileList(site_name, weather_dir):
	"""Given the (site_name) and the directory to search for files (weather_dir),
	a relative path, return a list of all .txt files containing the site name"""
	return [b for b in os.listdir(weather_dir) if '.txt' in b and site_name in b]
	
def GetNCDC_df(weather_dir, ncdc_file):
	"""Given an NCDC file, open the text file, and return a dataframe"""
	f = open(os.path.join(weather_dir, ncdc_file)).readlines()
	lines_of_data = False #we know the first several lines of the file can be removed
	NCDC_as_dic = {} #blank dictionary
	for line in f: #iterate over the lines of the file
		if 'SkyCondition' in line: #if this is the row of columns
			column_list = line.strip().split(',')
			lines_of_data = True #everything hereafter is viable data
			for c in column_list: #add a key in the dictionary for each attribute
				NCDC_as_dic[c] = [] #empty list, to be appended with each line of data
		elif lines_of_data and len(line) > 1: #if this is a line of data
			split_line = line.strip().split(',')
			for s,c in zip(split_line, column_list): 
				try: #if we can coerce the string to float
					NCDC_as_dic[c].append(float(s))
				except ValueError: #if we cannot, just append the string
					NCDC_as_dic[c].append(s)
	return pd.DataFrame(NCDC_as_dic)

def BuildSiteDataFrame(weather_dir, all_files):
	"""Open each member of (all_files) in (weather_dir) and return one full data frame"""
	for ind, f in enumerate(all_files):
		print "Reading file %d, %s" % (ind, f)
		if ind == 0: #if this is the first file:
			site_df = GetNCDC_df(weather_dir, f)
		else: #add this data frame to the previous one:
			site_df = site_df.append(GetNCDC_df(weather_dir,f))
			site_df.index = range(len(site_df)) #re-index 
	return site_df
	
def GetType(w):
	"""Return a mapped value for numerous types of weather to one heading.
	More information found at: http://cdo.ncdc.noaa.gov/qclcd/qclcddocumentation.pdf."""
	if 'SN' in w or 'FZ' in w: #snow or freezing rain
		return 'SN'
	elif 'RA' in w or 'TS' in w: #various forms of rain
		return 'RA'
	elif 'FG' in w or 'HZ' in w or 'BR' in w: #fog, haze, mist
		return 'FG'
	else:
		return ' ' #clear weather
		
def RoundToNearestNth(val, N, dec):
	"""Given a (val), round to the nearest (N)th fraction to (dec) decimal places,
	for instance, 100.139 to the nearest 20th, to 3 places, is: 100.150."""
	frac = int((val - int(val)) * N + 0.5) 
	return round(int(val) + float(frac)/N, dec)

def ConvertWeatherDate(w_date, w_time, N, dec, d_i_m = days_in_months, ls = leaps):
	"""Convert a (w_date) in YYYYMMDD format and a (w_time) in 0000 (<2400) format
	to a date of YYYYDOY.XXX... to (dec) decimal places rounded to the nearest (N)
	the of a day."""
	year = int(w_date/10000)
	month = int((w_date - 10000 * year)/100)
	day = int((w_date - 10000 * year - 100 * month))
	if month > 2 and year in ls: #if this is a leap year to consider
		day_of_year = d_i_m[month-2] + day
	elif month == 1: #this is January date
		day_of_year = day - 1
	else: #This is a non-January date, that is not impacted by leap years
		day_of_year = d_i_m[month-2] + day - 1 #so Jan 1st is 2012000.XXX, e.g.
	time = RoundToNearestNth((w_time - w_time % 100)/2400 + (w_time % 100)/60/24, N, dec) #get fractional time of day, rounded...
	return int(year * 1000 + day_of_year) + time

def ShortestDist(LatLon_df, Lat, Lon):
	"""Given a (Lat), a (Lon) and a (LatLon_df) containing columns of lats and lons, return the row
	of that data frame corresponding to the site closest to Lat,Lon."""
	distances = [(Lat-x)**2 + (Lon-y)**2 for x,y in zip(LatLon_df.Lat, LatLon_df.Lon)]
	return distances.index(np.min(distances))
	
def GetWSiteName(D, a, RoadwayCoordsDic):
	"""For a given pair_id (a), with the relevant dictionary to store paths (D), either we already
	know which site is closest - this could be a preprocessing step - or we need to search a database
	for the closest weather gauge.  This function will return the closest site as a string bearing its
	name, 'Bedford', e.g."""
	if D['weather_site_name'] != 'closest': #i.e. if this is already filled with a site name
		return D['weather_site_name']
	else: #we need to choose the appropriate NCDC climate gauge
		w_site_coords = pd.read_csv(os.path.join(D['bt_path'],"WeatherSite_Coords.csv"))
		if str(a) in RoadwayCoordsDic.keys(): #if these roadways' coordinates are listed
			lat, lon = RoadwayCoordsDic[str(a)]['Lat'], RoadwayCoordsDic[str(a)]['Lon']
			return w_site_coords.Site[ShortestDist(w_site_coords, lat, lon)]
		else:
			return D['weather_site_default']
			
def GetWeatherData(weather_dir, site_name):
	"""Given the (site_name) of the relevant weather site ("BostonAirport", e.g.), and the
	(weather_dir) in which they are found, return the full data frame."""
	if os.path.exists(os.path.join(weather_dir, site_name + "_NCDC.csv")):
		return pd.read_csv(os.path.join(weather_dir, site_name + "_NCDC.csv"))
	else: #if the relevant .csv file must be generated (generally a 5-10 second process)
		file_list = GetRelevantFileList(site_name, weather_dir)
		full_site = BuildSiteDataFrame(weather_dir, file_list)
		full_site.to_csv(os.path.join(weather_dir, site_name + "_NCDC.csv"), index = False)
	
if __name__ == "__main__":
	script_name, site_name = sys.argv
	D = BTA.HardCodedParameters()
	weather_dir = D['weather_dir']
	file_list = GetRelevantFileList(site_name, weather_dir)
	full_site = BuildSiteDataFrame(weather_dir, file_list)
	full_site.to_csv(os.path.join(weather_dir, site_name + "_NCDC.csv"), index = False)