import numpy as np
import pandas as pd
import geopandas
from datetime import date, timedelta
from common import (
    query_mssql_iteratively,
    get_elastic_index,
    SCFH_TO_SLPM_FACTOR,
    P_CUBED_CRS,
    create_emission_source_query_report_id,  ## added to get the Report Id
)
from config import (
    CUSTOMER,
    GLOBAL_LISA_TO_LEAK_CONVERSION_RATE,
    SUPER_EMITTER_SLPM,
    ITALY_PROJECTION_COORD_REF_SYSTEM,
)


# Get the emission sources

## Load non-reproccesed emission sources


non_reprocessed_emission_sources = pd.read_pickle(
    f"data/non-reprocessed-emission-sources-{CUSTOMER}-no-re-calculation.pickle"
)


## If Re-Processed Emission Sources

emission_sources_recalculated = pd.read_pickle(
    f"data/non-reprocessed-emission-sources-recalculated{CUSTOMER}.pickle"
)

# Concatenate the two dataframes

frames = [
    emission_sources_recalculated,
    non_reprocessed_emission_sources,
]
emission_sources = pd.concat(frames)


## Convert to Datetime


emission_sources = emission_sources.rename(
    columns={
        "PlumeEpochEnd": "SurveyEndDateTime",
        "PlumeEpochStart": "DateReportStarted",
    }
)

emission_sources["DateReportStarted"] = pd.to_datetime(
    emission_sources["DateReportStarted"]
)
emission_sources["SurveyEndDateTime"] = pd.to_datetime(
    emission_sources["SurveyEndDateTime"]
)

## Get Leak Investigations

if CUSTOMER == "italgas":
    # elasticsearch_index = "italgas_leak_investigation"
    elasticsearch_index = "italgas_g2g_leak_investigation"
elif CUSTOMER == "toscana energia":
    elasticsearch_index = "toscana_leak_investigation"
else:
    raise ValueError("CUSTOMER should be 'italgas' or 'toscana energia'")


leak_investigations_raw = get_elastic_index(elasticsearch_index)


report_ids = emission_sources.ReportId.unique().tolist()

leak_columns_to_keep = [
    "PeakNumber",
    "Region",
    "City",
    "PCubedReportName",
    "BoxId",
    # "LeakGrade",
    "codiceDispersione",
    # "FoundDateTime",
    "dataLocalizzazione",
    # "AG/BG",
    "aereoInterrato",
    # "LeakLocation",
    "indirizzoLocalizzazione",
    "LeakCoordLatLon",
    "PCubedReportGuid",
    "LeakFound",
    "PipelineMeters",
    "AssetCoverageFrac",
    "PriorityScore",  ## added for the emission sources solution
]

leak_investigations = (
    leak_investigations_raw.query("PCubedReportGuid.isin(@report_ids)")[
        leak_columns_to_keep
    ]
    .reset_index(drop=True)
    .copy()
)


leak_investigations["PeakNumber"] = (
    leak_investigations["PeakNumber"]
    .str.rsplit("LISA ")
    .map(lambda x: x[-1])
    .astype(int)
)

# leak_investigations["FoundDateTime"] = pd.to_datetime(
#     leak_investigations["FoundDateTime"]
# )
leak_investigations["dataLocalizzazione"] = pd.to_datetime(
    leak_investigations["dataLocalizzazione"], unit="s", infer_datetime_format=True
)
### Following the discussion with Noah:
# (https://picarro.slack.com/archives/D034LGQ0FV2/p1649962711721239?thread_ts=1649958068.663449&cid=D034LGQ0FV2)
# - I am renaming the PrioritiyScore and PeakNumber of the "leak_investigations" to don't eliminate them #but at least to differentiate them. I add a "leak" at the end of PriorityScore and PeakNumber
# - Don't consider the cells below that are commented. They will be eliminated once everything works.
    
    
leak_investigations.rename(
    columns={"PriorityScore": "PriorityScore_leak", "PeakNumber": "PeakNumber_leak"},
    inplace=True,
)


def get_latitude(x):
    if type(x) != list:
        lat = np.nan
    else:
        lat = x[0]
    return lat


def get_longitude(x):
    if type(x) != list:
        lon = np.nan
    else:
        lon = x[1]
    return lon


leak_investigations["LeakLatitude"] = (
    leak_investigations["LeakCoordLatLon"]
    .str.split(",")
    .map(get_latitude)
    .astype(float)
)
leak_investigations["LeakLongitude"] = (
    leak_investigations["LeakCoordLatLon"]
    .str.split(",")
    .map(get_longitude)
    .astype(float)
)



### Merge the emission sources with the leak investigations


leak_and_emission_source_join = (
    emission_sources.set_index("PeakNumber")
    .drop(index=0)
    .reset_index()
    .merge(
        leak_investigations,
        how="left",
        left_on=["ReportId", "PeakNumber"],
        right_on=["PCubedReportGuid", "PeakNumber_leak"],
        validate="1:m",
    )
)

non_reported_emission_sources = (
    emission_sources.set_index("PeakNumber").loc[0].reset_index()
)
leak_and_emission_source_join = pd.concat(
    [leak_and_emission_source_join, non_reported_emission_sources], axis=0
)


## Get Leak Probability


lisa_to_leak_probability_map = {
    "Found_Gas_Leak": 1,
    "Found_Other_Source": 0,
    "No_Gas_Found": 0,
    "Not_Investigated": GLOBAL_LISA_TO_LEAK_CONVERSION_RATE,
    "In_Progress": GLOBAL_LISA_TO_LEAK_CONVERSION_RATE,
}

leak_and_emission_source_join["LeakProbability"] = (
    leak_and_emission_source_join["LeakFound"]
    .map(lisa_to_leak_probability_map)
    .fillna(GLOBAL_LISA_TO_LEAK_CONVERSION_RATE)
)


leak_and_emission_source_join["LeakProbability"].value_counts(dropna=False)
reports_before_dropping_no_nat_gas = leak_and_emission_source_join["ReportId"].unique()

## Get Natural Gas or Possible Natural Gas LISAs




leak_and_emission_source_join = (
    leak_and_emission_source_join.set_index("DispositionName")
    .drop(index=["Not_Natural_Gas"])
    .reset_index()
)

reports_after_dropping_no_nat_gas = leak_and_emission_source_join["ReportId"].unique()


not_nat_gas_list = set(reports_before_dropping_no_nat_gas) - set(reports_after_dropping_no_nat_gas)


### Load the log

log_output = pd.read_excel(f"data/log-output-{CUSTOMER}.xlsx")

NumberOfReportsAfterNoNatGas = len(not_nat_gas_list)
log_output["NumberOfReportsAfterNoNatGas"] = NumberOfReportsAfterNoNatGas

if NumberOfReportsAfterNoNatGas == 0:
    
    log_output["ListOfReportsAfterNoNatGas"] = pd.Series("No reports with only not_natural_gas sources", dtype='object')
else:
    log_output["ListOfReportsAfterNoNatGas"] = pd.Series(list(not_nat_gas_list), dtype='object')

log_output.to_excel(f"data/log-output-{CUSTOMER}.xlsx")


### save

leak_and_emission_source_join.to_pickle(
    f"data/prepared-leaks-with-emission-sources-{CUSTOMER}.pickle"
)