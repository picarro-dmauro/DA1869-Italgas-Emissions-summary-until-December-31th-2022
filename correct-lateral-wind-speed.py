import matplotlib.pyplot as plt
from datetime import datetime
import glob
import numpy as np
import pandas as pd
from common import (
    create_peak_query,
    query_mssql_iteratively,
    get_measurements,
    create_survey_query,
)
from config import CUSTOMER

pd.options.mode.chained_assignment = None
plt.rcParams.update({"font.size": 14})



emission_sources = pd.read_pickle(
    f"data/non-reprocessed-emission-sources-{CUSTOMER}-re-calculation.pickle"
)

emission_source_ids = emission_sources.EmissionSourceId.drop_duplicates()

### Get the peaks
peaks = query_mssql_iteratively(emission_source_ids, create_peak_query)

### Get the survey_ids
survey_ids = emission_sources["SurveyId"].drop_duplicates()
surveys = query_mssql_iteratively(survey_ids, create_survey_query)


for j, survey in surveys.iterrows():
    start = datetime.now()

    measurements = get_measurements(
        survey["AnalyzerId"], survey["StartEpoch"], survey["EndEpoch"]
    )
    peaks_slice = peaks[peaks["SurveyId"] == survey["SurveyId"]]

    ws_lat = []
    ws_lon = []
    wind_inst_e = []
    wind_inst_n = []
    ws_rotation = []

    for i, row in peaks_slice.iterrows():
        measurements_subslice = measurements[
            (measurements["EpochTime"] >= row["PlumeEpochStart"])
            & (measurements["EpochTime"] <= row["PlumeEpochEnd"])
        ]
        ws_lat.append(measurements_subslice["WindSpeedLateral"].median())
        ws_lon.append(measurements_subslice["WindSpeedLongitudinal"].median())
        wind_inst_e.append(measurements_subslice["WindInstEast"].median())
        wind_inst_n.append(measurements_subslice["WindInstNorth"].median())
        ws_rotation.append(measurements_subslice["WeatherStationRotation"].median())

    peaks_slice["WS_LAT"] = ws_lat
    peaks_slice["WS_LON"] = ws_lat
    peaks_slice["WIND_INST_E"] = wind_inst_e
    peaks_slice["WIND_INST_N"] = wind_inst_n
    peaks_slice["WS_ROTATION"] = ws_rotation

    peaks_slice.to_csv(
        f"wind_speed_rotations/{CUSTOMER}-survey-{survey['SurveyId']}.csv"
    )
    print(f"Completed Survey {j + 1} of {len(surveys)} in {datetime.now() - start}")


peak_files = glob.glob(
    f"wind_speed_rotations/{CUSTOMER}-survey-*.csv"
)
peak_dfs = []
for f in peak_files:
    df_temp = pd.read_csv(f)
    peak_dfs.append(df_temp)
peaks_from_raw_data = pd.concat(peak_dfs)

fig = plt.figure(figsize=(12, 10))
ax = (
    peaks_from_raw_data.set_index("EpochTime")
    .sort_index()["WS_ROTATION"]
    .plot(figsize=(12, 10))
)
fig.savefig(f"data/WS_rotation.jpg", dpi=400, bbox_inches="tight")



inlet_scaling = 1.5
unit_factor = 0.1272

car_vector = [
    (i + 1j * j)
    for i, j in zip(
        peaks_from_raw_data["CarSpeedNorth"].values,
        peaks_from_raw_data["CarSpeedEast"].values,
    )
]

peaks_from_raw_data["CAR_SPEED_COMPONENT_IN_WIND"] = np.absolute(car_vector) * np.sin(
    np.radians(peaks_from_raw_data["WS_ROTATION"])
)

peaks_from_raw_data["WS_LAT_CORRECTED"] = (
    peaks_from_raw_data["WS_LAT"] - peaks_from_raw_data["CAR_SPEED_COMPONENT_IN_WIND"]
)

peaks_from_raw_data["PlumeEmissionRate_CORRECTED"] = (
    np.abs(peaks_from_raw_data["WS_LAT_CORRECTED"])
    * peaks_from_raw_data["LineIntegral"]
    * inlet_scaling
    * unit_factor
)

total_plume_emission_rate = peaks_from_raw_data["PCubedPlumeEmissionRate"].sum()
total_plume_emission_rate_corrected = peaks_from_raw_data[
    "PlumeEmissionRate_CORRECTED"
].sum()
print(total_plume_emission_rate)
print(total_plume_emission_rate_corrected)
pct_change_plume_emission = (
    100
    * (total_plume_emission_rate_corrected - total_plume_emission_rate)
    / total_plume_emission_rate
)
print(f"% change in total plume emission rate: {pct_change_plume_emission}")

bins = np.linspace(-2, 2, 201)
plt.hist(peaks_from_raw_data["WS_LAT"], bins=bins, histtype="step", label="WS_LAT")
plt.hist(
    peaks_from_raw_data["WS_LAT_CORRECTED"],
    bins=bins,
    histtype="step",
    label="WS_LAT_CORRECTED",
)
plt.legend()
plt.grid()
plt.show()
plt.savefig("WS_LAT_CORRECTED.jpg", dpi=400, bbox_inches="tight")



bins_log = np.logspace(-3, 3, num=70, endpoint=True, base=10.0, dtype=None, axis=0)
plt.hist(
    peaks_from_raw_data["PCubedPlumeEmissionRate"],
    bins=bins_log,
    histtype="step",
    density=False,
    cumulative=False,
    label="PlumeEmissionRate",
)
plt.hist(
    peaks_from_raw_data["PlumeEmissionRate_CORRECTED"],
    bins=bins_log,
    histtype="step",
    density=False,
    cumulative=False,
    label="PlumeEmissionRate_CORRECTED",
)
plt.semilogx()
plt.legend()
plt.grid()
plt.show()
plt.savefig("PlumeEmissionRate_CORRECTED.jpg", dpi=400, bbox_inches="tight")

peaks_from_raw_data.to_csv(
    f"wind_speed_rotations/corrected-peak-emissions-{CUSTOMER}.csv"
)