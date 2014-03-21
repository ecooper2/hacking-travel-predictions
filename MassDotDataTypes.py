"""This set of scripts will read, parse, and describe the various types of data associated with
Boston transportation data, including more detailed descriptions of the variables used."""

import os
import pandas as pd
import numpy as np
import NCDC_WeatherProcessor as NCDC

def GetRoadVolume_Historical(file_path, Cleaned, file_name):
	"""(Cleaned) is a boolean variable describing whether a pre-developed data frame has already
	been produced.  (file_path) denotes a relative path to the cleaned or uncleaned file.
	
	LocID: Identifier of the location, Ex: 1000, type = int
	County: Which Mass. county contains the roadway, Ex: MIDDLESEX, type = str
	Community: Which Mass. community contains the roadway, Ex: Billerica, type = str
	On: Name of roadway segment from which measurements were taken, Ex: MIDDLESEX TURNPIKE, type = str
	From: CURRENTLY VOID OF DATA
	To: CURRENTLY VOID OF DATA
	Approach: Description the stretch of roadway approaching the sensor, Ex: SOUTH OF, type = str
	At: Location of sensor, Ex: BEDFORD BILLERIC TOWN L, type = str
	Dir: Is the road in one or two directions?, Ex: "2-Way", type = str
	Latitude: Latitude of sensor location, Ex: 42.52414, type = float
	Longitude: Longitude of sensor location, Ex: -71.25355, type = float
	Latest: Most recent volume measurement in car/day, Ex: 5600, type = int
	Latest_Date: Date at which the latest volume measurement was taken, Ex: '20060101', type = int
	"""
	
	type_dict = {'Loc ID' : 'int',
				'County' : 'str',
				'Community' : 'str',
				'On' : 'str',
				'From' : 'str',
				'To' : 'str',
				'Approach' : 'str',
				'At' : 'str',
				'Dir' : 'float',
				'Latitude' : 'float',
				'Longitude' : 'float',
				'Latest': 'int',
				'Latest_Date' : 'int'}

	days_in_month = np.cumsum([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]) #to covert into day_of_year		
	leap_years = [1900 + 4*x for x in range(50)] #runs until 2096 for potential leap_years
				
	if not Cleaned: #if we are going to need to parse a given file
		f = open(os.path.join(file_path, file_name)).readlines()
		Road_Volumes_df = {}
		col_list = f[0].strip().split(',') #maintains order, even if dictionaries do not
		for col in col_list: #assume a header will define the names of columns
			Road_Volumes_df[col] = [] #empty list to be filled by examples or filler
		for line in f[1:]: #grab the data from the remaining rows in the file
			split_line = line.strip().split(',')
			for val,col in zip(split_line,col_list): #add an element to the list
				var_type = type_dict[col] #is this an 'int', 'str', etc?
				if val == "": #if this is blank, fill with a default for the var_type
					if var_type == 'str':
						Road_Volumes_df[col].append("no_data")
					else: #whether the var is a float or int, a '-999' is acceptable
						Road_Volumes_df[col].append(-999)
				elif 'Date' in col: #if this is a date that requires conversion
					Road_Volumes_df[col].append(SlashDateToNumerical(val, days_in_month, leap_years))
				else:
					Road_Volumes_df[col].append(val)
		Road_Volumes_df = pd.DataFrame(Road_Volumes_df)
		#remove '.csv', and add 'Cleaned'
		Road_Volumes_df.to_csv(os.path.join(file_path, file_name + "_Cleaned.csv"),
								index = False) 
		return Road_Volumes_df
	else: #if the file is already cleaned - simply read it into memory and return it
		return pd.read_csv(file_path, file_name + "_Cleaned.csv")
		
def GetBlueToad(file_path, Cleaned, file_name):
	"""(Cleaned) is a boolean variable describing whether a pre-developed data frame has already
	been produced.  (file_path) denotes a relative path to the cleaned or uncleaned file. 

	pair_id: Identifies a pair of bluetooth sensors in a particular direction, Ex: 60, type = int
	insert_time: The time at which the measurement was made, Ex: 20120613.609, type = float
	travel_time: The time in seconds it takes cars to travel the road segment between two sensors, Ex: 742, type = int
	"""	

	type_dict = {'pair_id' : 'int',
				'insert_time' : 'float',
				'travel_time' : 'int'}	
				
	days_in_month = np.cumsum([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]) #to covert into day_of_year		
	leap_years = [1900 + 4*x for x in range(50)] #runs until 2096 for potential leap_years
	
	if not Cleaned: #if we are going to need to parse a given file
		BlueToad_df = pd.read_csv(os.path.join(file_path, file_name))
		#Now, convert our dates to the relevant format
		cleaned_dates = []
		print "Converting Dates"
		for i in BlueToad_df.insert_time: 
			slash_date, colon_time = i.split(" ")
			num_date = SlashDateToNumerical(slash_date, days_in_month, leap_years) + ColonTimeToDecimal(colon_time)
			num_date = NCDC.RoundToNearestNth(num_date, 288, 3)
			cleaned_dates.append(num_date)
		BlueToad_df.insert_time = cleaned_dates #replace with suitable numerical, ordinal dates
		del(cleaned_dates) #to conserve memory
		BlueToad_df.to_csv(os.path.join(file_path, file_name + "_Cleaned.csv"),
							index = False)
		return BlueToad_df
	else: #if the file is already cleaned - simply read it into memory and return it
		return pd.read_csv(os.path.join(file_path, file_name + "_Cleaned.csv"))

def CleanBlueToad(BlueToad_df, file_path, file_name):
	"""Having converted BlueToad dates, remove those rows from the data frame in which the listed
	travel time is '\\N'."""
	###now, remove all non-numerical values from BlueToad_df.travel_time
	print "Removing Erroneous Data"
	BlueToad_df = BlueToad_df[BlueToad_df.travel_time != "\\N"] #not numbers? remove 'em!
	BlueToad_df.to_csv(os.path.join(file_path, file_name + "_Cleaned.csv"),
						index = False)
	return BlueToad_df
	
def FloatConvert(BlueToad_df, file_path, file_name):
	"""Convert the travel_time column from strings to floats."""
	print "Reformatting Travel Times As Floats..."
	BlueToad_df.travel_time = BlueToad_df.travel_time.astype('float64')
	print "Writing to File"
	BlueToad_df.to_csv(os.path.join(file_path, file_name + "_Cleaned.csv"),
						index = False)
	return BlueToad_df

def SlashDateToNumerical(date, days_in_month, leap_years):
	"""Converts a date of the form MM/DD/YYYY or YYYY-MM-DD and return YYYYDOY"""
	if "/" in date: #if this a "MM/DD/YYYY" date
		month, day, year = date.split("/")
	elif "-" in date: #if this is a "YYYY-MM-DD"
		year, month, day = date.split("-")
	else:
		return date
	if month == '1' or month == '01': #a January date
		DOY = int(day)
	else:
		DOY = days_in_month[int(month) - 2] + int(day)
	if int(year) in leap_years and int(month) > 2: #if this is a leap_year-impacted date
		return int(year) * 1000 + DOY
	else:
		return int(year) * 1000 + DOY - 1 #thus, the first day of the year is 0

def ColonTimeToDecimal(time):
	"""Converts a HH:MM:SS (HH in military time) to a decimal between 0 and 1"""
	split_time = time.split(":")
	if len(split_time) == 2: #if this is solely HH:MM
		hour, minute = split_time; second = '0'
	else:
		hour, minute, second = split_time
	return float(hour)/24 + float(minute)/60/24 + float(second)/60/60/24