#!/usr/bin/env python

"""Provide a simple method to get traffic estimations for a large amount of
keywords where the Web-Based Google Keyword Planner tool would be impractical.

"""

from __future__ import division
import csv
import sys
import time

from adspygoogle import AdWordsClient

__all__ = (
    "get_traffic_estimates",
)

_CLIENT = None # To Do
_MAX_KEYWORD_REQUESTS = 500


def _chunks(iterable, chunk_size):
    for index in xrange(0, len(iterable), chunk_size):
        yield iterable[index: index + chunk_size]


def get_traffic_estimates(client, input_filepath, output_filepath=None,
                          location_ID=2826, language_ID=1000):
    """Reads keyword data from a CSV file, queries Google API's
    TrafficEstimatorService for each keyword and then outputs traffic and cost
    data to a CSV file.

    Arguments:
    input_filepath - The full absolute filepath of the CSV input file.
    output_filepath (Keyword Default: None) - The full absolute filepath for
    for the generated CSV output file. If no value is passed, a timestamped
    file name will be used as default.
    location_ID (Keyword Default: 2826) - The Location ID to be used for the
    traffic estimations as seen on
    https://developers.google.com/adwords/api/docs/appendix/geotargeting. If
    no value is passed, United Kingdom will be used as default.
    language_ID (Keyword Default: 1000) - The Language ID to be used for the
    traffic estimations as seen on
    https://developers.google.com/adwords/api/docs/appendix/languagecodes. If
    no value is passed, English will be used as default.

    """
    dir_path = os.path.dirname(input_filepath)
    keyword_estimate_requests = []
    with open(input_filepath, "rb") as input_file:
        for row in csv.DictReader(input_file):
            keyword_estimate_requests.append({
                "keyword": {
                    "xsi_type": "Keyword",
                    "matchType": row["Type"],
                    "text": row["Keyword"]},
                "maxCpc": {
                    "xsi_type": "Money",
                    "microAmount": 1000000 * row["Max CPC"]}
                })
    sys.stdout.write("Keyword Traffic Estimator: CSV contents succesfully "
                     "imported. Querying TrafficEstimatorService for data...")
    traffic_estimator_service = client.GetTrafficEstimatorService(
                                version="v201306")
    keywords_data = []
    for index, keywords in enumerate(_chunks(keyword_estimate_requests,
                                             _MAX_KEYWORD_REQUESTS)):
        estimates = traffic_estimator_service.get({
            "campaignEstimateRequests": [{
                "adGroupEstimateRequests": [{
                    "keywordEstimateRequests": keyword_estimate_requests
                    }],
                "criteria": [{
                    "xsi_type": "Location",
                    "id": location_ID
                },
                {
                    "xsi_type": "Language",
                    "id": language_ID
                }]
            }]})[0]
        keyword_estimates = estimates["campaignEstimates"][0][
                            "adGroupEstimates"][0]["keywordEstimates"][0]
        for estimate in reversed(keyword_estimates):
            keyword = keyword_estimate_requests.pop()
            keywords_data.append({
                "Keyword": keyword,
                "Monthly Impressions": ((estimate["min"]["impressionsPerDay"]
                                         + estimate["max"][
                                         "impressionsPerDay"]) / 2) * 30.4,
                "Monthly Clicks": ((estimate["min"]["clicksPerDay"] +
                                    estimate["max"]["clicksPerDay"]) / 2)
                                    * 30.4,
                "CTR": (estimate["min"]["clickThroughRate"] +
                        estimate["max"]["clickThroughRate"]) / 2,
                "Average CPC": ((estimate["min"]["averageCpc"]["microAmount"]
                                 + estimate["max"]["averageCpc"][
                                 "microAmount"]) / 2) / 1000000,
                "Cost": ((estimate["min"]["totalCost"] +
                          estimate["max"]["totalCost"]) / 2) / 100000,
                "Average Position": (estimate["min"]["averagePosition"] +
                                     estimate["max"]["averagePosition"]) / 2
                })
    sys.stdout.write("Keyword Traffic Estimator: Crunching Batch %s" % index +
                     1)
    keywords_data.reverse()
    if output_filepath is None:
        output_filepath = os.path.join(dir_path, "Keyword Traffic Estimates"
                                       " - %s.csv" % time.strftime("%d-%m-%Y"))
    field_names = keywords_data[0].iterkeys()
    with open(output_filepath, "wb") as output_file:
        csv_dict_writer = csv.DictWriter(output_file, field_names)
        csv_dict_writer.writerow({field_name: field_name
                                  for field_name in field_names})
        for keyword_data in keywords_data:
            csv_dict_writer.writerow(keyword_data)
    sys.stdout.write("Keyword Traffic Estimator: Data successfully output to "
                     "%s" % output_filepath)


if __name__ == "__main__":
    if 1 <= len(sys.argv) <= 4:
        sys.exit("Keyword Traffic Estimator: Syntax - <csv_input_filepath> "
                 "[csv_output_filepath] [location ID] [language ID]")
    get_traffic_estimations(_CLIENT, *sys.argv)

'''Example Usages:

keyword_traffic_estimator.py C:/Users/Anon/KW.csv
keyword_traffic_estimator.py C:/Users/Anon/KW.csv C:/Users/Anon/Output.csv
keyword_traffic_estimator.py C:/Users/Anon/KW.csv C:/Users/Anon/KW2.csv 20342
'''

''' TO DO:
- Implement OAuth2.0 Client Authorisation.
- Allow Negatives to be passed to refine traffic estimations?
- Provide more user-friendly error handling.
'''