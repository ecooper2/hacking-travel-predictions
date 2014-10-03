# hacking-travel-predictions

Generates roadway travel time and speed predictions for the next 24 hours based on [real time traffic data](http://traffichackers.com/current.json) and a [historical archive of past traffic](https://github.com/hackreduce/MassDOThack/blob/master/Road_RTTM_Volume/massdot_bluetoad_data.zip).

## Background

Traffic delays are tremendously annoying and immensely wasteful.  For those of us commuting winding circuits through the world's larger metropolitan agglomerations, we experience these delays on a regular basis.  To cope, we generally spew epithets into the air, laced with the appropriate regional flavors of disgust and stress (also carbon dioxide).  While many of us have the computational wherewithal use Google Maps and its ilk to show us the current level of roadway suffering, solutions to our everyday commuting problems are far less ubiquitous.

What are these problems?  Whether we sit on our family room couches eating bowls of cereal while watching Mike & Mike in the Morning or sit in our office cubicles during the late afternoon, we are not interested in the question for which there are a plethora of answers: “what does the traffic look like now?” The real questions we, the commuters, have are twofold:

1.  What will the traffic be like at location x, in y minutes?
2.  If I leave earlier or later, will my commute be materially improved?

To this end, we have developed a simple traffic-predictor, allowing users to see projected traffic conditions up to twenty-four hours in advance.  Better still, the estimates consider the current weather conditions, giving you the heads up when

Are the estimates perfect?  In a word, no.  Are they a significant improvement on the status quo?  We think so, and we'd love to hear what you think.

## Getting started

Make sure you have [Python (2.7.x)](https://www.python.org/downloads/) and [pip](https://pip.pypa.io/en/latest/installing.html) installed, then:

1.  Install python modules

  ```
  $ pip install -r requirements.txt
  ```

2.  Run the predictive model from scratch

  ```
  $ python BlueToadAnalysis.py today scratch.txt -w -t
  ```

The above BlueToadAnalysis.py command runs the model from scratch, downloading supporting data as necessary, and generated predictions based on current traffic, the current day of the week, and the current weather.

## Reference

The main module is BlueToadAnalysis.py, called primarily from the command line with two required arguments and three optional arguments:

The first defines the day (or days) considered in prediction generation.  Each option is described below:

  * **today** - Run the model, estimating conditions for the current day of the week (i.e Thursday).
  * **weekday** - Consider all weekdays (Monday thru Friday)
  * **weekend** - Consider all weekend days (Saturday and Sunday)
  * **(monday, tuesday, ..., sunday)** - Run the model using desired conditions using only historical examples from the listed day.
  
The second names the output file into which a JSON will be written (in the /update directory), containing the output predictions.

The four options (-w or -weather), (-t or -traffic), (-l or -length), and (-hr and -hour) are optional arguments instructing the model to include:
  - Weather.  Each historical example is classified as snow/ice, rainstorms, fog/haze, or clear.  Including this
  option ensures that examples from which predictive estimates emerge are of the same weather classification as the
  current conditions, via NOAA's nearest gauge: http://w1.weather.gov/xml/current_obs/seek.php?state=ma&Find=Find

  - Traffic.  Consider only historical examples with similarly free-flowing/congested traffic conditions.
  
  - Length of forecast.  If left blank, 288 five-minute predictions are generated (one day).  Otherwise, the number of 5-minute increments can
    be chosen.
	
  - Hour in HH:MM format, which will be rounded to the nearest five-minute time-stamp.  If this option is entered, weather will be ignored
    (regardless of the value chosen above), traffic will be ignored (regardless of the value chosen), and the model will simply generated
	estimates based on the chosen day(s) of the week and the chosen time of the day.
