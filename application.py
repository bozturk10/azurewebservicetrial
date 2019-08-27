# Kemal Sedat Ceyhan
# Python code to get hourly weather information for each Sabanci factory
# Supervisors: Burak Gurbuz, Ali Koc

# /Users/sedatceyhan/Desktop/SabanciDX
# /Users/sedatceyhan/jython2.7.0/jython.jar

from __future__ import print_function
import json
from flask import Flask
import time
import requests


#from googlegeocoder import GoogleGeocoder
import datetime
import pandas as pd

import asyncio
from aiohttp import ClientSession

HEADERS={ "X-RapidAPI-Host": "dark-sky.p.rapidapi.com", "X-RapidAPI-Key": "31006866damsh45a64a87415d34fp1687f6jsneb50ece9bbfc"}

# Method that returns a tuple with 2 items: First is a dictionary with all the addresses as keys and the values are
# dictionaries with only current unix timestamps as keys and another dictionary with weather info as values.
# e.g., {address ---> current time ----> {temperature : x, pressure: y, ....} for each address
# The second item is a similar object, but contains the weather info for 48 hours for each address
# e.g., {address ---> time 1 ----> {temperature : x, pressure: y, ....}
#                     time 2 ----> {temperature : x, pressure: y, ....}
#                      ...
#                     time 48 ----> {temperature : x, pressure: y, ....}}

app = Flask(__name__)
weather_df = pd.read_csv("adres.csv")
address_list_all = list(weather_df['Adres'])
geo_list_all = [(41.0852407, 29.0101336), (40.8659193, 29.38637709999999), (41.08494899999999, 29.010165), (41.0853334, 29.01034899999999), (41.0852407, 29.0101336), (41.027361, 29.124344), (41.0255247, 29.1010561), (41.0001235, 29.0993972), (41.0001235, 29.0993972), (39.926787, 32.853788), (41.0001235, 29.0993972), (37.040769, 35.31484), (39.074979, 26.941987), (40.6898, 41.4106), (40.7491747, 40.2399573), (38.206537, 37.193076), (37.534111, 35.519312), (37.848064, 36.873216), (38.033333, 37.083333), (37.56458600000001, 35.52977500000001), (37.635051, 35.594031), (37.734614, 35.764471), (37.683333, 35.766667), (37.848064, 36.873216), (37.8861377, 36.1844053), (39.74704, 28.000306), (39.874299, 26.238592), (36.8289137, 33.4682813), (40.14671999999999, 26.408587), (40.910369, 32.721718), (40.353707, 27.971405), (39.074979, 26.941987), (40.7642481, 29.9918153), (38.1878811, 36.2681656), (40.7679787, 29.9942867), (40.9213476, 29.3158738), (35.1134472, -85.2441575), (34.808964, -79.524964), (40.4440458, -75.3512906), (33.8484006, -117.9718064), (33.1380279, -117.1865708), (-12.6586454, -38.2992841), (-6.5018086, 106.8779776), (14.3321749, 100.6442889), (31.230878, 121.464561), (40.7628723, 29.9978603), (40.75103900000001, 30.414263), (41.0218508, 29.0502427), (41.020933, 29.050565), (36.9967732, 35.2365099), (41.28236, 28.00059), (41.085, 29.00999999999999), (41.0147935, 28.5622437), (39.874299, 26.238592), (40.9313734, 35.8739403), (39.96600300000001, 33.107213), (39.849255, 30.9861981), (38.855062, 35.852098), (36.8012474, 34.6289539), (37.94982280000001, 34.6814333), (38.661139, 30.586276), (53.4647359, 9.9859713), (35.1331987, 33.9345815), (45.61166, 13.81119), (44.2247171, 28.6285535), (38.3339722, -0.4899658), (44.7019511, 37.7411361), (41.0182171, 29.055166), (38.193843, 27.362074)]
final_json= ""



async def get_json_info(geo_list, address_list):
    # Dictionary that assigns (lat, long) object to human readable string address
    latLong_to_address_dict = {}
    # The returned tuple's second item as described above
    facility_dict = {}
    # The returned tuple's first item as described above
    facility_dict_current = {}
    # After the manipulation, we get a list of strings where each list item is a unique factory address

    #geocoder = GoogleGeocoder("AIzaSyA-FW3li4gI_epJs8tAwhCXuhdSm-mwTOQ")

    # The list that contains (lat, long) tuple of each string address
    #latLong_List = []

    j = 0
    i = 1
    for address in address_list:
        # For each address we assign an empty dictionary to fill out later.
        # First one is to hold the hourly data, and the second for the current hour's data
        facility_dict[address] = {}
        facility_dict_current[address] = {}

        # Google.geocode object of the string address
        #search = geocoder.get(str(address))[0].geometry.location
        search = geo_list[j]

        # Assign the string representation of the address as a value to get it later
        latLong_to_address_dict[search] = address

        #latLong_List.append(search)
        print(str(search) + ",")
        j +=1
        i += 1

    # We iterate through all the addresses where each address is in Google's geocode format
    for loc in geo_list:
        # if time.time() - start_time >= 200:
        #     return str(json.dumps(facility_dict_current, indent=4, sort_keys=True)) + "11111111------" + str(
        #         time.time() - start_time)
            # Flag variable to get the current hour's weather information, and assign it to each address as its value.(e.g., facility_dict_current[address]---> )
        # When we fetch the current hour's weather info we change the variable to FALSE
        is_initial_time = True

        # String representation of the current address
        curr_address = latLong_to_address_dict[loc]

        # here we play with the google geocode location to get the data as a list containing
        # the curr address's lat and long, e.g., string_coords = [latitude, longitude]
        #string_coords = str(loc)[1:-1]
        #coords = string_coords.split(",")

        lat = loc[0]
        long = loc[1]

        # We get the weather information of the given location
        rev_loc = str(lat) + "," + str(long)
        url="https://dark-sky.p.rapidapi.com/" + str(rev_loc) + "?lang=en&units=auto"
        async with ClientSession() as session:
            async with session.get(url, headers=HEADERS) as response:
                resp = await response.json()

        # Data containing 48 hour of weather information for the given address
        # The first hour in this data is in fact the current hour.(The hour at which we start querying)
        # The last item of this data is the weather info of 48 hours after the initial time of query.

        two_day_data = resp['hourly']['data']

        # For each hour in 48 hours
        for curr_hour in two_day_data:
            # if time.time() - start_time >= 200:
            #     return str(json.dumps(facility_dict_current, indent=4, sort_keys=True)) + "2222222------" + str(
            #         time.time() - start_time)
                # Empty dict that is initialized every hour in order to hold the weather info of
            # whatever hour we are at
            curr_dict = {}

            # We assign the relevant values
            curr_time = curr_hour['time']

            curr_dict['humidity'] = curr_hour['humidity']
            curr_dict['pressure'] = curr_hour['pressure']

            curr_dict['temperature'] = curr_hour['temperature']
            curr_dict['wind_speed'] = curr_hour['windSpeed']
            curr_dict['wind_bearing'] = curr_hour['windBearing']

            # We separately hold the current hour's weather info
            # This is done at the beginning, and only once for each address
            # Thus we change the flag boolean's val to False
            if is_initial_time:
                facility_dict_current[curr_address][curr_time] = curr_dict
                is_initial_time = False

            # For each address we are holding its weather info for every hour
            # Here we have separate keys for each hour, and for each hour we have its weather info
            facility_dict[curr_address][curr_time] = curr_dict

            # if time.time() - start_time >= 200:
            #     return str(json.dumps(facility_dict_current, indent=4, sort_keys=True)) + "3333333------" + str(
            #         time.time() - start_time)

        init_time = list(facility_dict[curr_address].keys())[0]
        # print(init_time)
        # print(facility_dict[curr_address][init_time])

    #print(json.dumps(facility_dict_current, indent=4, sort_keys=True))
    #print('\n')
    #print(json.dumps(facility_dict, indent=4, sort_keys=True))

    print('\n')
    # return (facility_dict_current, facility_dict)
    # return (json.dumps(facility_dict_current, indent=4, sort_keys=True), json.dumps(facility_dict, indent=4, sort_keys=True))
    global final_json
    final_json += str(json.dumps(facility_dict_current, indent=4, sort_keys=True))





#@app.route("/")
#def run_it():
#   return get_json_info()

async def main():
    await asyncio.gather(get_json_info(geo_list_all[:5], address_list_all[:5]), get_json_info(geo_list_all[5:10],address_list_all[5:10]), get_json_info(geo_list_all[10:15], address_list_all[10:15]),get_json_info(geo_list_all[15:20], address_list_all[15:20]),get_json_info(geo_list_all[20:25], address_list_all[20:25]),
                         get_json_info(geo_list_all[25:30], address_list_all[25:30]),get_json_info(geo_list_all[30:35], address_list_all[30:35]),get_json_info(geo_list_all[35:40], address_list_all[35:40]),
                         get_json_info(geo_list_all[40:45], address_list_all[40:45]),get_json_info(geo_list_all[45:50], address_list_all[45:50]), get_json_info(geo_list_all[50:55], address_list_all[50:55]),
                         get_json_info(geo_list_all[55:60], address_list_all[55:60]), get_json_info(geo_list_all[60:65], address_list_all[60:65]), get_json_info(geo_list_all[65:], address_list_all[65:]))

import time
s = time.perf_counter()
loop = asyncio.get_event_loop()
loop.run_until_complete(main())

@app.route("/")
def run_it():
    return final_json

#run_it()
#print(final_json)
