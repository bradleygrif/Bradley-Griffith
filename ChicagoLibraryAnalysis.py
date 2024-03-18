#region Imports
import json
import requests
import pandas as pd
#endregion

#region Define the url for the city of Chicago library data. Read the data from that API
library_data_url = "https://data.cityofchicago.org/resource/x8fc-8rcq.json"

library_jsondata = requests.get(library_data_url).json()

library_data = pd.DataFrame(library_jsondata)

library_data = library_data[['name_', 'hours_of_operation', 'address', 'city', 'state', 'zip', 'phone', 'website', 'location']]

#endregion

#region We have to do the same process to grab the data for the policy districts, in order to get zip codes
police_station_data_url = "https://data.cityofchicago.org/resource/z8bn-74gv.json"

police_station_jsondata = requests.get(police_station_data_url).json()

police_station_data = pd.DataFrame(police_station_jsondata)
police_station_data['district'] = police_station_data['district'].apply(lambda x : str(x).zfill(3))
#endregion

#region The process for reading in crime data is similar, but I can only read it in 1000 records at a time, since that is the limit on their API. This process takes a while
limit = 1000
x = True
count = 0
crime_data = pd.DataFrame()

crime_data_url = "https://data.cityofchicago.org/resource/ijzp-q8t2.json?$limit={limit}&$offset={offset}"

while x:
    crime_jsondata = requests.get(crime_data_url.format(limit = limit, offset = count * limit)).json()
    new_crime_data = pd.DataFrame(crime_jsondata)
    new_crime_data = new_crime_data[['id', 'case_number', 'date', 'block', 'iucr', 'primary_type',
       'description', 'location_description', 'arrest', 'domestic', 'beat',
       'district', 'ward', 'community_area', 'fbi_code', 'x_coordinate',
       'y_coordinate', 'year', 'updated_on', 'latitude', 'longitude']]
    crime_data = pd.concat([crime_data, new_crime_data])
    print(count)
    count = count + 1
    if len(crime_data) == 0:
        x = False
#endregion


crime_with_zip = pd.merge(left=crime_data, right = police_station_data, left_on= 'district', right_on = 'district')
#combined_library_crime_data = pd.merge(left = crime_with_zip , right = library_data, how = 'left', on = 'zip')

crime_count_by_zip_by_type = crime_with_zip.groupby(['zip', 'primary_type']).size().reset_index()
crime_count_by_zip = crime_with_zip.groupby(['zip']).size().reset_index()
crime_count_by_zip = crime_count_by_zip.rename(columns={0: "Number of Crimes"})

library_count_by_zip = library_data.groupby(['zip']).size().reset_index()
library_count_by_zip = library_count_by_zip.rename(columns={0: "Number of Libraries"})

number_of_crimes_by_number_of_libraries = pd.merge(crime_count_by_zip, library_count_by_zip, on = 'zip')
