import json
import pdb
import requests
import collections
from datetime import datetime

from requests.auth import HTTPBasicAuth


class Event:
    def __init__(self, url, timestamp):
        self.url = url
        self.timestamp = timestamp


def get_events_data(user, API_KEY):
    headers = {'Accept': 'application/json'}
    api_url = "https://candidate.hubteam.com/candidateTest/v3/problem/dataset?userKey={}".format(user)
    response = requests.request("get", api_url, headers=headers)
    return response.json()


def post_sessions_view_data(data, user):
    headers = {'Accept': 'application/json'}
    api_url = "https://candidate.hubteam.com/candidateTest/v3/problem/result?userKey={}".format(user)
    response = requests.post(api_url, json=data)
    pdb.set_trace()


def group_and_sort_events_by_user(events_data):
    events_by_user = {}
    # Create a dict with userId as key and value as set of events for that user
    for event in events_data['events']:
        events_by_user.setdefault(event['visitorId'], [])
        events_by_user[event['visitorId']].append(Event(event['url'], event['timestamp']))

    for user in list(events_by_user):
        # sort the event list based on timestamp per user
        orig_list = events_by_user[user]
        orig_list.sort(key=lambda x: x.timestamp)
        events_by_user['user'] = orig_list
    return events_by_user


def construct_session(events_sub_list):
    duration = events_sub_list[-1].timestamp - events_sub_list[0].timestamp
    start_time = events_sub_list[0].timestamp
    pages = []
    for event in events_sub_list:
        pages.append(event.url)
    return {"duration": duration, "pages": pages, start_time: start_time}


def create_sessions_from_events_list_per_user(events):
    sessions = []
    index = 0

    while index + 1 < len(events):
        session = {}
        start_index = index
        end_index = start_index + 1
        session_start = datetime.fromtimestamp(events[start_index].timestamp / 1000.0)
        session_end = datetime.fromtimestamp(events[end_index].timestamp / 1000.0)
        diff_minutes = (session_end - session_start).total_seconds() / 60.0
        while diff_minutes <= 10.0 and end_index + 1 < len(events) - 1:
            end_index += 1
            session_end = datetime.fromtimestamp(events[end_index + 1].timestamp / 1000.0)
            diff_minutes = (session_end - session_start).total_seconds() / 60.0

        # one session will consist of events from start_index to end_index

        if (end_index - start_index > 1):
            sessions.append(construct_session(events[start_index:end_index]))
        else:
            sessions.append(
                {"duration": 0, "pages": events[start_index].url, "startTime": events[start_index].timestamp})
        index = end_index
    return sessions


def create_sessions_view(events_by_user_data):
    sessionsByUser = {}
    for user in events_by_user_data.keys():
        events_list = events_by_user_data[user]
        user_sessions = create_sessions_from_events_list_per_user(events_list)
        sessionsByUser[str(user)] = user_sessions

    #print (json.dumps({json.dumps("sessionsByUser"):json.dumps(sessionsByUser)}))
    return json.dumps({"sessionsByUser":sessionsByUser})


API_KEY = "0f39d5ab33ac84bb5c4fa0428b39"
events_data = get_events_data("0f39d5ab33ac84bb5c4fa0428b39", API_KEY)
events_by_user_data = group_and_sort_events_by_user(events_data)
sessions_view_data = create_sessions_view(events_by_user_data)
print(json.dumps(sessions_view_data))

post_sessions_view_data(sessions_view_data, API_KEY)
