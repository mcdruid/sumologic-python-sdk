# Submits search job, waits for completion, then prints and emails _messages_
# (as opposed to records).  Pass the query via stdin.
#
# cat query.sumoql | python search-job-messages.py <accessId> <accessKey> \
# <fromDate> <toDate> <timeZone> <byReceiptTime>
#
# Note: fromDate and toDate must be either ISO 8601 date-times or epoch
#       milliseconds
#
# Example:
#
# cat query.sumoql | python search-job-messages.py 2019-09-30T00:00:00 2019-09-30T23:59:59

import json
import sys
import time
import os
from os import path

# for debugging
# import pprint
# pp = pprint.PrettyPrinter(indent=4)

from sumologic import SumoLogic

if path.isfile("access.key"):
    cf = open("access.key", "r")
    creds = cf.readlines()
else:
    sys.exit("access.key file missing. Place your accessId and accessKey on separate lines in this file.")

sumo = SumoLogic(creds[0].strip(), creds[1].strip())
args = sys.argv
fromTime = args[1]
toTime = args[2]
timeZone = 'UTC'
byReceiptTime = False
r_fields = ['_messagetime', 'msg'] # names of fields to include in output

delay = 5
q = ' '.join(sys.stdin.readlines())
sj = sumo.search_job(q, fromTime, toTime, timeZone, byReceiptTime)

status = sumo.search_job_status(sj)
while status['state'] != 'DONE GATHERING RESULTS':
    if status['state'] == 'CANCELLED':
        break
    time.sleep(delay)
    print('.', end = '')
    status = sumo.search_job_status(sj)

print(status['state'])

if status['state'] == 'DONE GATHERING RESULTS':
    count = status['messageCount']
    print("retrieved " + str(count) + " results")

f = open("sumo_results_" + fromTime + "_to_" + toTime + ".txt", "a+")
batch = 10000 # The maximum value for limit is 10,000
offset = 0

while offset < count:
    print("writing results " + str(offset) + " to " + str(offset + batch))
    r = sumo.search_job_messages(sj, batch, offset)

    row = {}
    for res in r['messages']:
        rf = res['map']
        for fld in r_fields:
            row[fld] = rf[fld]

        f.write(json.dumps(row) + "\n")

    offset = offset + batch

f.close()
