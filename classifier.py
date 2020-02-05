import pandas as pd

'''
# generate state-specific csv files of geocoordinates
df_acc = pd.read_csv("US_Accidents.csv")        # import us accidents
states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
for state in states:                            # for each state, create state-local dataframe
    df_state = df_acc[df_acc["State"]==state]   # consisting of lats & longs
    df_state = df_state[["Start_Lat", "Start_Lng"]]
    #with open(state + ".csv", mode)            # write state-local data frame to each file
    df_state.to_csv("./States/" + state + ".csv", index=False, header=True) # with state abbreviation as file name
'''

'''
import requests # check if api key is valid
api_key = "Not shown for security reasons"
url_base = "https://maps.googleapis.com/maps/api/geocode/json?address=1600+Amphitheatre+Parkway,+Mountain+View,+CA&key="
url = url_base + api_key
r = requests.get(url)
results = r.json()
status = results.get('status')
if status!="OK": 
    print("Invalid api key. Terminating script.") # if invalid, exit program
    quit()

import googlemaps
gmaps_key = googlemaps.Client(key = api_key) # access google maps api w/ api key

df_customers = pd.read_csv("US_Customers.csv")  # import customer information
df_customers["cus_lat"] = None                  # add customer lattitude col
df_customers["cus_lon"] = None                  # add customer longitude col

customers_sz = len(df_customers)
for i in range(customers_sz): # for each customer, update lat and long based on customer's adr
    geocode_result = gmaps_key.geocode(df_customers.loc[i, "Address"])
    df_customers.loc[i, "cus_lat"] = geocode_result[0]["geometry"]["location"]["lat"]
    df_customers.loc[i, "cus_lon"] = geocode_result[0]["geometry"]["location"]["lng"]

df_customers["Acc_Num"] = 0 # add num of accidents col to customers data frame

from math import radians, cos, sin, asin, sqrt # convert geocoordinates to distance in miles
def miles(lat1, lon1, lat2, lon2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlat = lat2 - lat1 
    dlon = lon2 - lon1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return 3958.756 * 2 * asin(sqrt(a)) # earth radius = 3958.756 mi

for i in range(customers_sz): # for each customer
    cus_lat = df_customers.loc[i, "cus_lat"]
    cus_lon = df_customers.loc[i, "cus_lon"]
    avg = df_customers.loc[i, "Avg_Miles"]
    customer_state = df_customers.loc[i, "State"]

    df_state = pd.read_csv("./States/" + customer_state + ".csv")
    state_sz = len(df_state)
    for j in range(state_sz): # for each accident in customer's state
        acc_lat = df_state.loc[j, "Start_Lat"]
        acc_lon = df_state.loc[j, "Start_Lng"]
        d_acc = miles(cus_lat, cus_lon, acc_lat, acc_lon) # calculate distance of accident
        if d_acc < avg: # if accident's distance falls within customer's daily driving distance
            df_customers.loc[i, "Acc_Num"] += 1 # increment number of accidents for that customer

del df_customers["cus_lat"]
del df_customers["cus_lon"]

df_customers = df_customers.sort_values(by=["Acc_Num"])
#print(df_customers[["Name", "Acc_Num"]])

def divide(nums, avg):
    lesser = []
    greater = []
    for num in nums:
        if num < avg: lesser.append(num)
        else: greater.append(num)
    return lesser, greater

import statistics
acc_nums = df_customers["Acc_Num"].tolist() # conv acc_num col in customers data frame to list
med5 = statistics.median(acc_nums) # get median

lesser, greater = divide(acc_nums, med5)
med2_5 = statistics.median(lesser)

lesser, greater = divide(lesser, med2_5)
med1_2_5 = statistics.median(lesser)
med3_7_5 = statistics.median(greater)

lesser, greater = divide(acc_nums, med5)
med7_5 = statistics.median(greater)

lesser, greater = divide(greater, med7_5)
med6_2_5 = statistics.median(lesser)
med8_7_5 = statistics.median(greater)

df_customers["Level"] = None # add risk col to customers data frame
df_customers["Risk"] = None # add risk col to customers data frame
for i in range(customers_sz):
    acc = df_customers.loc[i, "Acc_Num"] # get the customer's acc_num
    if med3_7_5 < acc < med6_2_5: 
        if acc < med5: df_customers.loc[i, "Level"] = 4 # use acc_num to categorize 
        else: df_customers.loc[i, "Level"] = 5
        df_customers.loc[i, "Risk"] = "Average" # use acc_num to categorize 
    elif med6_2_5 <= acc:                                                # customer's risk level
        if acc < med7_5: 
            df_customers.loc[i, "Level"] = 6
            df_customers.loc[i, "Risk"] = "Moderately High"
        elif acc < med8_7_5: 
            df_customers.loc[i, "Level"] = 7
            df_customers.loc[i, "Risk"] = "High"
        else: 
            df_customers.loc[i, "Level"] = 8
            df_customers.loc[i, "Risk"] = "Very High"
    else: # acc <= med3_7_5
        if med2_5 < acc: 
            df_customers.loc[i, "Level"] = 3
            df_customers.loc[i, "Risk"] = "Moderately Low"
        elif med1_2_5 < acc: 
            df_customers.loc[i, "Level"] = 2
            df_customers.loc[i, "Risk"] = "Low"
        else: 
            df_customers.loc[i, "Level"] = 1
            df_customers.loc[i, "Risk"] = "Very Low"

df_customers.to_csv("Customer_Data.csv", index=False, header=True)
'''

import gspread 
from oauth2client.service_account import ServiceAccountCredentials
scope = ["https://www.googleapis.com/auth/spreadsheets", 
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name("client_secret.json", scope)
client = gspread.authorize(credentials)
content = open("Customer_Data.csv", "r").read()
#sheet_id = "Not shown for security reasons"
client.import_csv(sheet_id, content.encode("utf-8"))
