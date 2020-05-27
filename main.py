import requests
import time
import boto3

# API endpoint for the users data
endpoint='https://x37sv76kth.execute-api.us-west-1.amazonaws.com/prod/users'
# Pagination variable for the API endpoint.  Starting from 0, get all results until an empty array is returned.
# TODO: To get incremental data, may need to track the last pagination id that was processed successfully and start from that id +1
page = 0
# Starting value for exponential backoff in case of API throttling
sleep = 1

s3_resource = boto3.resource('s3')
bucket_name = '9dt-jayljohnson'

# TODO: Should keep the pagination number and processing timestamp in the data.  
#  (cont.) In the future if need to fetch players that were updated, may need to pass paagination number to scan less data for the updates.
def get_players(endpoint, page):
    payload = {'page': page}
    r = requests.get(endpoint, params=payload)
    return r

def split_json_array_elements_to_new_lines(input_array):
    result = ""
    for i in input_array.json():
        if len(result) == 0:
            result = str(i)
        else:
            result = result + '\n' + str(i)
    return result

# Initialize players as a list with a dummy value so that the while loop can begin.
# TODO: improve this so that network traffic isn't required for local testing
players = get_players(endpoint, page) 

# Keep processing until an empty array is returned by the API.  Empty array signals the last paginated result was returned.
while players.json(): 
    players = get_players(endpoint, page)
    status = players.status_code
    # Successful API call (status 200) that returns a non-empty json array (players.json())
    if status == 200 and players.json():
        # when status 200 success, reset default sleep to 1 second.  Used only for API throttling backoff
        sleep = 1
        # Each json result in the array needs to be on a new line for Athena/Spectrum support. 
        # TOOD: Test if this can be skipped when this table option is enabled: ignore.malformed.json 
        players_formatted = split_json_array_elements_to_new_lines(players)
        
        # Write the results to S3.
        # TODO: For performance, combine multiple API call results up to a threshold and then write to S3
        #       This can also make Athena/Spectrum reads more efficient
        s3_resource.Bucket(bucket_name).put_object(Key = f'players/{page}.json', Body = players_formatted)

        print(f"Success writing results page {page}")
        page += 1
    # If the array is empty it is the last page of records and there is no more data.  Break from the while loop.
    elif not players.json():
        print(f"Last page of results received; exiting")
        break
    # In case of API throttling or other issues, retry with backoff
    else:
        print(f"API call status code: {str(status)}")
        print(f"Waiting for {sleep} seconds")
        time.sleep(sleep)
        # doubles the sleep time with each non-200 response. TODO: Verify the status_code when API throttles, is it 429?
        sleep = sleep*2         
        if sleep > 8:
            print(f"Exceeded retry limit at page {page}, exiting")
            break