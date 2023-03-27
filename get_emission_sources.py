
from dotenv import load_dotenv
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from common import (
    format_ax,
    query_mssql_iteratively,
    create_emission_source_query,
    PASSWORD,
)
from config import CUSTOMER, FINAL_REPORTS_PATH, FINAL_REPORT_COLUMN, REPORTS_AND_MONTHS

print(CUSTOMER)



#### TO DO: Insert the number of the month up to which you need to compute the emissions

current_month = 9
list_months = list(np.arange(1, current_month, 1))  # list of months

list_months_re_calculation = [1,2,3,4,5]
list_months_no_re_calculation = [6,7,8,9,10,11,12]




### Load the reports
report_prefixes_with_date = pd.read_excel(
    FINAL_REPORTS_PATH,
)[REPORTS_AND_MONTHS]

assert (
    len(report_prefixes_with_date[FINAL_REPORT_COLUMN])
    == report_prefixes_with_date[FINAL_REPORT_COLUMN].nunique()
)
print(len(report_prefixes_with_date[FINAL_REPORT_COLUMN]))






### Months that need recalculation

report_prefixes_with_date["Driving Completion Date"] = pd.to_datetime(
    report_prefixes_with_date["Driving Completion Date"]
)

months_to_be_recalculated = report_prefixes_with_date[
    report_prefixes_with_date["Driving Completion Date"].dt.month.isin(
        list_months_re_calculation
    )
]


### Months that do not need recalculation

report_prefixes_with_date["Driving Completion Date"] = pd.to_datetime(
    report_prefixes_with_date["Driving Completion Date"]
)


months_not_to_be_recalculated = report_prefixes_with_date[
    report_prefixes_with_date["Driving Completion Date"].dt.month.isin(
        list_months_no_re_calculation
    )
]

### Check that the report are unique for re calculation

report_prefixes_re_calculation = (
    months_to_be_recalculated[FINAL_REPORT_COLUMN].str.split("-").map(lambda x: x[-1])
)
assert len(report_prefixes_re_calculation) == report_prefixes_re_calculation.nunique()
print(len(report_prefixes_re_calculation), report_prefixes_re_calculation.nunique())

### Check that the report are unique for no re calculation

report_prefixes_no_re_calculation = (
    months_not_to_be_recalculated[FINAL_REPORT_COLUMN]
    .str.split("-")
    .map(lambda x: x[-1])
)

assert (len(report_prefixes_no_re_calculation) == report_prefixes_no_re_calculation.nunique())
print(len(report_prefixes_no_re_calculation))

assert (len(report_prefixes_no_re_calculation) + len(report_prefixes_re_calculation)) == len(report_prefixes_with_date)

smart_sheet_num_of_reports = len(report_prefixes_with_date)

# Get the emission sources for the two set of reports

## re_calculation reports

print("Taking the reports that need recalculation...")

emission_sources_re_calculation = query_mssql_iteratively(
    query_parameters=report_prefixes_re_calculation,
    query_function=create_emission_source_query,
)

print("Done with the reports that need recalculation...")

number_of_reports_after_LSDB_re_calc = emission_sources_re_calculation[
    "ReportId"
].nunique()



list_number_of_reports_after_LSDB_re_calc = emission_sources_re_calculation[
    "ReportName"
].unique()
list_number_of_reports_after_LSDB_re_calc = list(
    list_number_of_reports_after_LSDB_re_calc
)


## Check if there is anything from 2021

emission_sources_2021_re_calculation_index = emission_sources_re_calculation[emission_sources_re_calculation["Year"] == 2021].index
emission_sources_2021_re_calculation = emission_sources_re_calculation[emission_sources_re_calculation["Year"] == 2021]

# Get report names of 2021 for the reports that need recalculation and get the number of unique reports after querying the LS database

report_ids_2021_re_calc = emission_sources_2021_re_calculation["ReportId"].unique()
number_of_reports_after_emissions_re_calc = emission_sources_2021_re_calculation["ReportId"].nunique()


### Dropping results from 2021

emission_sources_re_calculation.drop(emission_sources_2021_re_calculation_index, inplace=True)
number_of_report_after_removing_2021_re_calculation = emission_sources_re_calculation["ReportId"].nunique()

### Saving to pickle file the reports that need re-calculation

print("Saving the reports that need recalculation...")
emission_sources_re_calculation.to_pickle(f"data/non-reprocessed-emission-sources-{CUSTOMER}-re-calculation.pickle")



# Take the reports that do not need recalculation

print("Taking the reports that do not need recalculation...")

emission_sources_no_re_calculation = query_mssql_iteratively(
    query_parameters=report_prefixes_no_re_calculation,
    query_function=create_emission_source_query,)

number_of_reports_after_LSDB_no_re_calc = emission_sources_no_re_calculation["ReportId"].nunique()


list_number_of_reports_after_LSDB_no_re_calc = emission_sources_no_re_calculation[
    "ReportName"
].unique()
list_number_of_reports_after_LSDB_no_re_calc = list(
    list_number_of_reports_after_LSDB_no_re_calc
)


number_of_reports_after_DB = (
    list_number_of_reports_after_LSDB_re_calc
    + list_number_of_reports_after_LSDB_no_re_calc
)



list_of_reports_not_in_DB = list(set(report_prefixes_with_date["Finalreportpcubed"]) - set(
    number_of_reports_after_DB)
)




print("Done with the reports that do not need recalculation...")


emission_sources_2021_no_re_calculation_index = emission_sources_no_re_calculation[emission_sources_no_re_calculation["Year"] == 2021].index
emission_sources_2021_no_re_calculation = emission_sources_no_re_calculation[emission_sources_no_re_calculation["Year"] == 2021]

report_ids_2021_no_re_calc = emission_sources_2021_no_re_calculation["ReportId"].unique()
number_of_reports_after_emissions_no_re_calc = emission_sources_2021_no_re_calculation["ReportId"].nunique()

print(report_ids_2021_no_re_calc, number_of_reports_after_emissions_no_re_calc)

## Check if any reports of 2021 are there and drop them

emission_sources_no_re_calculation_after = emission_sources_no_re_calculation.drop(emission_sources_2021_no_re_calculation_index)
number_of_report_after_removing_2021_no_re_calculation = (emission_sources_no_re_calculation_after["ReportId"].nunique())

## Count the number of reports after the emissions


NumberOfReportsLSdatabase = (number_of_reports_after_LSDB_re_calc + number_of_reports_after_LSDB_no_re_calc)

# NumberOfReportsAfterRemoving2021AndReportsWithLowPS = (number_of_report_after_removing_2021_re_calculation + number_of_report_after_removing_2021_no_re_calculation)

reports_2021 = report_ids_2021_re_calc.tolist() + report_ids_2021_no_re_calc.tolist()


# Checking if reports contain only drives from 2021



list_of_reports_with_all_drives_in_2021 = list(set(reports_2021) - set(
    number_of_reports_after_DB)
)




## Save to pickle the emission sources that do not need recalculation

print("Saving the reports that do not need recalculation...")
emission_sources_no_re_calculation.to_pickle(f"data/non-reprocessed-emission-sources-{CUSTOMER}-no-re-calculation.pickle")



### Create the excel log output:

columns_name = [
    "NumberOfReportsSmartSheet",
    "NumberOfReportsLSdatabase",
]

# if len(reports_2021) > len(list_of_reports_not_in_DB):
#     N_rows = len(reports_2021)
    
# elif ((len(reports_2021)) == 0 and (len(list_of_reports_not_in_DB) == 0)) :
#     N_rows = 1
# else:
#     N_rows = len(list_of_reports_not_in_DB) 

if ((len(reports_2021)) == 0 and (len(list_of_reports_not_in_DB) == 0)) :
    N_rows=1
else:    
    N_rows = max([len(reports_2021), len(list_of_reports_not_in_DB)])

N_cols = len(columns_name)


log_output = pd.DataFrame(np.zeros((N_rows,N_cols)), columns=columns_name, index=None)


log_output["NumberOfReportsSmartSheet"] = len(report_prefixes_with_date)
log_output["NumberOfReportsLSdatabase"] = NumberOfReportsLSdatabase

if len(report_prefixes_with_date) != NumberOfReportsLSdatabase:

    log_output["ReportsNotInLSdatabase"] = pd.Series(list_of_reports_not_in_DB, dtype='object')
else:
    log_output["ReportsNotInLSdatabase"] = pd.Series("all reports retrieved", dtype='object')
log_output["NumberOfReports2021"] = len(reports_2021)
# log_output["NumberOfReportsAfterRemoving2021AndReportsWithLowPS"] = NumberOfReportsAfterRemoving2021AndReportsWithLowPS

if len(reports_2021) == 0:
    pass
else:
    log_output["ListOfReports2021"] = pd.Series(reports_2021)

log_output.to_excel(f"data/log-output-{CUSTOMER}.xlsx")



