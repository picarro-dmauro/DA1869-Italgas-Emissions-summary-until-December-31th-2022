import numpy as np
import pandas as pd
from psanalytics.dto.Peak import Peak
from psanalytics.dto.EmissionSource import EmissionSource
import matplotlib.pyplot as plt
from datetime import datetime
import glob
from common import (
    create_peak_query,
    query_mssql_iteratively,
    get_measurements,
    create_survey_query,
)
from config import CUSTOMER

pd.options.mode.chained_assignment = None
plt.rcParams.update({"font.size": 14})



def compute_source_emission_rate(group):   
    em_temp = []
    peaks = []
    j = 0
    for i in range(len(group)):
        plume_emission_rate = group.iloc[i].PlumeEmissionRate_CORRECTED
        peak = Peak(
            PlumeEmissionRate=plume_emission_rate,
            #PlumeEmissionRateUncertainty=group.iloc[i].PlumeEmissionRateUncertainty,
        )
        peaks.append(peak)
        
    emission_source = EmissionSource(
        peaks=peaks,
    )
        
    j += 1
    emission_source.calc_rate_stats_from_measurements()
    
    emission_source.calc_emission_rate_prob_log_normal()
    em_source = emission_source.EmissionRate

  
    
    
    #print('"EmissionRate": ', emission_source.EmissionRate, ',')
    # print('"EmissionRateAMean": ', emission_source.EmissionRateAMean, ',')
    # print('"EmissionRateAStd": ', emission_source.EmissionRateAStd, ',')
    # print('"EmissionRateGMean": ', emission_source.EmissionRateGMean, ',')
    # print('"EmissionRateGStd": ', emission_source.EmissionRateGStd, ',')
    # print('"EmissionRateLowerBound": ', emission_source.EmissionRateLowerBound, ',')
    # print('"EmissionRateUpperBound": ', emission_source.EmissionRateUpperBound, ',')
    return em_source



## Get the emission sources and Peak Data

emission_sources = pd.read_pickle(
    f"data/non-reprocessed-emission-sources-{CUSTOMER}-re-calculation.pickle"
)



peaks_csv = pd.read_csv(
    f"wind_speed_rotations/corrected-peak-emissions-{CUSTOMER}.csv"
)
peaks_csv["EmissionSourceId"] = peaks_csv[
    "EmissionSourceId"
].str.lower()  # this is because the merging is case sensitive
# peaks_csv.info()
# peaks_csv.head()

emission_source_ids = (
    peaks_csv.EmissionSourceId.drop_duplicates()
)  # Here we get the emission source id of the peaks that have been corrected. This will be used to get the peaks of these corrections and recalculate the emissions


result = peaks_csv.groupby(by=["EmissionSourceId"], as_index=True).apply(
    compute_source_emission_rate
)


## Checking number of anomalies in case you see from the next step that there are some


anomalies = result[result.isna()]
list_emsourceid = anomalies.index.values.tolist()



for anomaly_emsourceid in list_emsourceid:
    num_peaks = (
        peaks_csv.query("EmissionSourceId == @anomaly_emsourceid")
        .groupby(["EmissionSourceId"])
        .size()
    )
    num_peaks_count = num_peaks[0]
    print(num_peaks_count)
    
    
n_sample = 30
for anomaly_emsourceid in list_emsourceid:
    print(anomaly_emsourceid)
    anomaly = (
        peaks_csv.query("EmissionSourceId == @anomaly_emsourceid")
        .sample(n=n_sample)
        .groupby(by=["EmissionSourceId"], as_index=True)
        .apply(compute_source_emission_rate)
    )
    print(anomaly)

    if anomaly[0] == np.nan:
        print("EmissionSourceId '{}' has an issue".format(anomaly_emsourceid))
        break
    else:
        print("ok")
        print("anomalies")
        result.loc[anomaly_emsourceid] = anomaly[0]
        
        
## Check if there are still nans

print("'{}' NaN values".format(len(result[result.isna()])))


result_df = pd.DataFrame(result, columns=["EmissionRate"]).reset_index()
result_df["EmissionSourceId"] = result_df["EmissionSourceId"].str.lower()


result_final = pd.merge(
    emission_sources, result_df, on="EmissionSourceId", validate="one_to_one"
)

result_final_save = result_final.drop("MeasuredSCFH", axis=1)
result_final_save.rename(columns={"EmissionRate": "MeasuredSCFH"}, inplace=True)


result_final_save.to_pickle(
    f"data/non-reprocessed-emission-sources-recalculated{CUSTOMER}.pickle"
)