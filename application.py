# Kemal Sedat Ceyhan
# Python code to get hourly weather information for each Sabanci factory
# Supervisors: Burak Gurbuz, Ali Koc

#/Users/sedatceyhan/Desktop/SabanciDX
#/Users/sedatceyhan/jython2.7.0/jython.jar

from __future__ import print_function
import json
from flask import Flask
import time
import requests

from googlegeocoder import GoogleGeocoder
import datetime
import pandas as pd

'''
def getFormatted(line):
    l = line.split(",")
    location = l[3]

    return (location)

'''
# Method that returns a tuple with 2 items: First is a dictionary with all the addresses as keys and the values are
# dictionaries with only current unix timestamps as keys and another dictionary with weather info as values.
# e.g., {address ---> current time ----> {temperature : x, pressure: y, ....} for each address
# The second item is a similar object, but contains the weather info for 48 hours for each address
# e.g., {address ---> time 1 ----> {temperature : x, pressure: y, ....}
#                     time 2 ----> {temperature : x, pressure: y, ....}
#                      ...
#                     time 48 ----> {temperature : x, pressure: y, ....}}

app = Flask(__name__)
def get_json_info():

    # Dictionary that assigns (lat, long) object to human readable string address
    latLong_to_address_dict = {}

    # The returned tuple's second item as described above
    facility_dict = {}

    # The returned tuple's first item as described above
    facility_dict_current = {}

    # After the manipulation, we get a list of strings where each list item is a unique factory address
    weather_df = pd.read_csv("data/Sabancı Holding Şirket Adresleri.csv")

    address_list = list(weather_df['Adres'])

    geocoder = GoogleGeocoder("AIzaSyA-FW3li4gI_epJs8tAwhCXuhdSm-mwTOQ")

    # The list that contains (lat, long) tuple of each string address
    latLong_List = []
    i = 1
    for address in address_list:

        # For each address we assign an empty dictionary to fill out later.
        # First one is to hold the hourly data, and the second for the current hour's data
        facility_dict[address] = {}
        facility_dict_current[address] = {}

        # Google.geocode object of the string address
        search = geocoder.get(str(address))[0].geometry.location

        # Assign the string representation of the address as a value to get it later
        latLong_to_address_dict[search] = address

        latLong_List.append(search)
        print(str(search) + ": " + str(i))
        i += 1

    # We iterate through all the addresses where each address is in Google's geocode format
    for loc in latLong_List:

        # Flag variable to get the current hour's weather information, and assign it to each address as its value.(e.g., facility_dict_current[address]---> )
        # When we fetch the current hour's weather info we change the variable to FALSE
        is_initial_time = True

        #String representation of the current address
        curr_address = latLong_to_address_dict[loc]

        # here we play with the google geocode location to get the data as a list containing
        # the curr address's lat and long, e.g., string_coords = [latitude, longitude]
        string_coords = str(loc)[1:-1]
        coords = string_coords.split(",")

        lat = coords[0]
        long = coords[1]

        # We get the weather information of the given location
        rev_loc = str(lat) + "," + str(long)
        response = requests.get("https://dark-sky.p.rapidapi.com/" + str(rev_loc) + "?lang=en&units=auto",
                                headers={
                                    "X-RapidAPI-Host": "dark-sky.p.rapidapi.com",
                                    "X-RapidAPI-Key": "be002cd988msh66f82e421ec7d23p115fdfjsn97ae91b9c263"
                                }
                                ).json()

        # Data containing 48 hour of weather information for the given address
        # The first hour in this data is in fact the current hour.(The hour at which we start querying)
        # The last item of this data is the weather info of 48 hours after the initial time of query.
        two_day_data = response['hourly']['data']

        # For each hour in 48 hours
        for curr_hour in two_day_data:
            # Empty dict that is initialized every hour in order to hold the weather info of
            # whatever hour we are at
            curr_dict = {}

            # We assign the relevant values
            curr_time = curr_hour['time']
            curr_humidity = curr_hour['humidity']
            curr_pressure = curr_hour['pressure']
            curr_temp = curr_hour['temperature']
            curr_wind_spd = curr_hour['windSpeed']
            curr_wind_bearing = curr_hour['windBearing']

            curr_dict['humidity'] = curr_humidity
            curr_dict['pressure'] = curr_pressure
            curr_dict['temperature'] = curr_temp
            curr_dict['wind_speed'] = curr_wind_spd
            curr_dict['wind_bearing'] = curr_wind_bearing

            # We separately hold the current hour's weather info
            # This is done at the beginning, and only once for each address
            # Thus we change the flag boolean's val to False
            if is_initial_time:
                facility_dict_current[curr_address][curr_time] = curr_dict
                is_initial_time = False

            # For each address we are holding its weather info for every hour
            # Here we have separate keys for each hour, and for each hour we have its weather info
            facility_dict[curr_address][curr_time] = curr_dict

        init_time = list(facility_dict[curr_address].keys())[0]
        #print(init_time)
        #print(facility_dict[curr_address][init_time])

    print(json.dumps(facility_dict_current, indent=4, sort_keys=True))
    print('\n')
    print(json.dumps(facility_dict, indent=4, sort_keys=True))

    print('\n')
    #return (facility_dict_current, facility_dict)
    #return (json.dumps(facility_dict_current, indent=4, sort_keys=True), json.dumps(facility_dict, indent=4, sort_keys=True))
    return str(json.dumps(facility_dict_current, indent=4, sort_keys=True))
    

#def get_info_from_time_and_loc(location, time):

def simpletrial():
    berk = {}
    berk['sefer'] = 'katil'
    berk['cennet_mah'] = 'not_katil'
    return str(berk)

'''
j = 1
for facility in facility_dict:
    print("ADDRESS: " + str(j))
    for time in facility_dict[facility]:
        print(
            datetime.datetime.fromtimestamp(
                int(time)
            ).strftime('%Y-%m-%d %H:%M:%S')
        )
        print(facility_dict[facility][time])

    j+=1
    print('\n\n')



'''


@app.route("/")
def run_it():
    #return get_json_info()
    berk = {}
    berk['sefer'] = 'katil'
    berk['cennet_mah'] = 'not_katil'
    return str(berk)
