import os
# Set environment variables
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'JsonFileName.json' # Change JSON File Name as needed

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    FilterExpressionList,
    Metric,
    MetricType,
    RunReportRequest,
)

import pandas as pd

property_id = "ID" # Change the property_id for each website
run_start_date = "30daysAgo" # Change start date to 30days Ago unless pulling for specific date range
run_end_date = "yesterday" # Change end date to yesterday unless pulling for specific date range

client = BetaAnalyticsDataClient()

request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="dateHour"),
            Dimension(name="customEvent:event_name_1"),
            Dimension(name="customEvent:event_name_2"),
            Dimension(name="pagePath"),
        ],
        metrics=[
            Metric(name="screenPageViews"),
            Metric(name="userEngagementDuration"),
            Metric(name="scrolledUsers"),
        ],
        date_ranges=[DateRange(start_date=run_start_date, end_date=run_end_date)],
        limit=100000,
        offset=0,
        dimension_filter=FilterExpression(
        or_group=FilterExpressionList(
                expressions=[
                    FilterExpression(
                        not_expression=FilterExpression(
                            filter=Filter(
                                field_name="customEvent:event_name_1",
                                string_filter=Filter.StringFilter(value="(not set)"),
                            )
                        )                    
                    ),
                    FilterExpression(
                        not_expression=FilterExpression(
                            filter=Filter(
                                field_name="customEvent:event_name_2",
                                string_filter=Filter.StringFilter(value="(not set)"),
                            )
                        )                    
                    )
                ]
            )
        )
    )

# print(request)

response = client.run_report(request)
# print(response)

run_row_count = response.row_count
print (run_row_count)

offset = 0
limit = 100000

dict = {
    "RowNum":[],
    "dateHour":[],
    "customEvent_event_name_1":[],
    "customEvent_event_name_2":[],
    "pagePath":[],
    "screenPageViews":[],
    "userEngagementDuration":[],
    "scrolledUsers":[]
}


if run_row_count>limit:
    while offset <= run_row_count:
        request = RunReportRequest(
            property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="dateHour"),
            Dimension(name="customEvent:event_name_1"),
            Dimension(name="customEvent:event_name_2"),
            Dimension(name="pagePath"),
        ],
        metrics=[
            Metric(name="screenPageViews"),
            Metric(name="userEngagementDuration"),
            Metric(name="scrolledUsers"),
        ],
            date_ranges=[DateRange(start_date=run_start_date, end_date=run_end_date)],
            limit=limit,
            offset=offset,
            dimension_filter=FilterExpression(
            or_group=FilterExpressionList(
                    expressions=[
                        FilterExpression(
                            not_expression=FilterExpression(
                                filter=Filter(
                                    field_name="customEvent:event_name_1",
                                    string_filter=Filter.StringFilter(value="(not set)"),
                                )
                            )                    
                        ),
                        FilterExpression(
                            not_expression=FilterExpression(
                                filter=Filter(
                                    field_name="customEvent:event_name_2",
                                    string_filter=Filter.StringFilter(value="(not set)"),
                                )
                            )                    
                        )
                    ]
                )
            ),
        )
        response = client.run_report(request)

        for dimensionHeader in response.dimension_headers:
            print(f"Dimension header name: {dimensionHeader.name}")
        for metricHeader in response.metric_headers:
            metric_type = MetricType(metricHeader.type_).name
            print(f"Metric header name: {metricHeader.name} ({metric_type})")


        for rowIdx, row in enumerate(response.rows):
            dict["RowNum"].append(rowIdx)
            for i, dimension_value in enumerate(row.dimension_values):
                raw_dimension_name = response.dimension_headers[i].name
                dimension_name = raw_dimension_name.replace(':','_')
                dict[dimension_name].append(dimension_value.value)

            for i, metric_value in enumerate(row.metric_values):
                metric_name = response.metric_headers[i].name
                dict[metric_name].append(metric_value.value)

        print(offset)
        offset=offset+100000
else:
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="dateHour"),
            Dimension(name="customEvent:event_name_1"),
            Dimension(name="customEvent:event_name_2"),
            Dimension(name="pagePath"),
        ],
        metrics=[
            Metric(name="screenPageViews"),
            Metric(name="userEngagementDuration"),
            Metric(name="scrolledUsers"),
        ],
        date_ranges=[DateRange(start_date=run_start_date, end_date=run_end_date)],
        limit=limit,
        offset=offset,
        dimension_filter=FilterExpression(
            or_group=FilterExpressionList(
                    expressions=[
                        FilterExpression(
                            not_expression=FilterExpression(
                                filter=Filter(
                                    field_name="customEvent:event_name_1",
                                    string_filter=Filter.StringFilter(value="(not set)"),
                                )
                            )                    
                        ),
                        FilterExpression(
                            not_expression=FilterExpression(
                                filter=Filter(
                                    field_name="customEvent:event_name_2",
                                    string_filter=Filter.StringFilter(value="(not set)"),
                                )
                            )                    
                        )
                    ]
                )
            ),
    )
    response = client.run_report(request)

    for dimensionHeader in response.dimension_headers:
        print(f"Dimension header name: {dimensionHeader.name}")
    for metricHeader in response.metric_headers:
        metric_type = MetricType(metricHeader.type_).name
        print(f"Metric header name: {metricHeader.name} ({metric_type})")


    for rowIdx, row in enumerate(response.rows):
        dict["RowNum"].append(rowIdx)
        for i, dimension_value in enumerate(row.dimension_values):
            raw_dimension_name = response.dimension_headers[i].name
            dimension_name = raw_dimension_name.replace(':','_')
            dict[dimension_name].append(dimension_value.value)

        for i, metric_value in enumerate(row.metric_values):
            metric_name = response.metric_headers[i].name
            dict[metric_name].append(metric_value.value)


print(type(dict))
df=pd.DataFrame.from_dict(dict)
print(df)

df.to_csv("Filename.txt", encoding='utf-8', index=False, sep='\t')
