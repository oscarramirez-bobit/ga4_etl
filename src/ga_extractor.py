from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from datetime import datetime, timedelta
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="config/credential.json"

# Initialize the GA Admin and GA Data API clients with the loaded credentials
admin_client = AnalyticsAdminServiceClient()
data_client = BetaAnalyticsDataClient()

def list_properties_by_account():
    """List all properties under each account accessible by the service account."""
    account_summaries = admin_client.list_account_summaries()

    all_properties = {}
    for account in account_summaries:
        account_id = account.name.split('/')[-1]  # Extract account ID
        properties_list = []
        for property_summary in account.property_summaries:
            properties_list.append({
                'property_id': property_summary.property.split('/')[-1],  # Extract property ID
                'display_name': property_summary.display_name,
                'property_name': property_summary.property
            })
        all_properties[account_id] = properties_list

    return all_properties


def fetch_ga4_data_for_property(property_id, start_date, end_date, metrics, dimensions):
    # Create the DateRange object
    date_range = DateRange(start_date=start_date, end_date=end_date)

    # Convert metric and dimension strings to Metric and Dimension objects
    metric_objs = [Metric(name=m) for m in metrics]
    dimension_objs = [Dimension(name=d) for d in dimensions]

    # Create the RunReportRequest
    request = RunReportRequest(property=f"properties/{property_id}",
                               date_ranges=[date_range],
                               metrics=metric_objs,
                               dimensions=dimension_objs)

    # Run the report and collect results
    response = data_client.run_report(request)
    results = []
    for row in response.rows:
        result = {dimension[row]: value.value for dimension, value in zip(dimensions, row.dimension_values)}
        result.update({metric[row]: value.value for metric, value in zip(metrics, row.metric_values)})
        results.append(result)
    
    return results

def fetch_ga4_data(property_ids, start_date, end_date, metrics, dimensions):
    all_results = []
    for property_id in property_ids:
        results = fetch_ga4_data_for_property(property_id, start_date, end_date, metrics, dimensions)
        for result in results:
            result['property_id'] = property_id  # Add property ID to each result for identification
            all_results.append(result)
    return all_results