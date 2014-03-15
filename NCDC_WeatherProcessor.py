"""This module should read all relevant NCDC weather files for a given site (user-specified), 
traversing the directory for those files and returning a single data-frame containing all 
relevant months."""

import sys
import os
import pandas as pd

global days_in_months 
global leaps
days_in_months = np.cumsum([31,28,31,30,31,30,31,31,30,31,30,31]) #for date conversion
leaps = [1900 + 4 * x for x in range(50)] #for leap year determination

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
	time = RoundToNearestNth(w_time/2400, N, dec) #get fractional time of day, rounded...
	return int(year * 1000 + day_of_year) + time
	
	
		
if __name__ == "__main__":
	script_name, site_name = sys.argv
	weather_dir = os.path.join("NCDC_Weather")
	file_list = GetRelevantFileList(site_name, weather_dir)
	full_site = BuildSiteDataFrame(weather_dir, file_list)
	full_site.to_csv(os.path.join(weather_dir, site_name + "_NCDC.csv"))