import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import lognorm, norm
from common import SCFH_TO_SLPM_FACTOR, plot_histogram, plot_cdf
from config import CUSTOMER, FINAL_REPORTS_PATH

WSU_MU_NORM = -1.36
WSU_SIGMA_NORM = 1.77
ITALGAS_EMISSION_FACTORS = {
    "B-2": 0.2,
    "B-1": 0.5,
    "B0": 1.9,
    "B1": 9.1,
}  # Italgas & Toscana, https://picarro.atlassian.net/wiki/spaces/DAKB/pages/2291859477/Emission+Factor+Lookup+Tables



## Get Natural Gas or Possible Natural Gas LISAs

emission_sources = pd.read_pickle(
    f"data/prepared-leaks-with-emission-sources-{CUSTOMER}.pickle"
)

## Fit Log-Normal Curve to Measured Emissions

shape_measured, _, scale_measured = lognorm.fit(emission_sources.MeasuredSCFH, floc=0)

mu_measured_norm = np.log(scale_measured)
sigma_measured_norm = shape_measured

mu_measured_norm, sigma_measured_norm


# Plot italgas emissios:


fig = plt.subplots(figsize=(12, 10))
ax, bins = plot_histogram(
    x=np.log(emission_sources.MeasuredSCFH),
    title=f"{CUSTOMER} Measured Emissions",
    x_label="$ln$(emission rate)",
    n_bins="sqrt",
    label=f"{CUSTOMER} $ln$(measurements)",
)

y_fit = norm.pdf(bins, loc=mu_measured_norm, scale=sigma_measured_norm)



ax.plot(
    bins,
    y_fit,
    linewidth=2,
    label=f"{CUSTOMER}"
    + r" normal: $\mu={:0.2f}$; $\sigma={:0.2f}$".format(
        mu_measured_norm, sigma_measured_norm
    ),
)

ax.plot(
    bins,
    norm.pdf(bins, loc=WSU_MU_NORM, scale=WSU_SIGMA_NORM),
    linewidth=2,
    label=r"WSU normal: $\mu={:0.2f}$; $\sigma={:0.2f}$".format(
        WSU_MU_NORM, WSU_SIGMA_NORM
    ),
)

ax.legend()
plt.savefig(CUSTOMER+"_Measured_Emission.jpg", dpi=400, bbox_inches="tight")



fig = plt.subplots(figsize=(12, 10))
ax, bins = plot_cdf(
    x=emission_sources.MeasuredSCFH,
    title=f"{CUSTOMER} Measured Emissions",
    x_label=r"emission rate ($\frac{ft^3}{hr}$)",
    n_bins=100000,
    log_x=True,
    log_y=False,
    label=f"{CUSTOMER} measurements",
    color=None,
)

y_fit = lognorm.cdf(bins, s=sigma_measured_norm, scale=np.exp(mu_measured_norm))

ax.plot(
    bins,
    y_fit,
    linewidth=2,
    label=f"{CUSTOMER}"
    + r" log-normal: $\mu={:0.2f}$; $\sigma={:0.2f}$".format(
        mu_measured_norm, sigma_measured_norm
    ),
)

ax.plot(
    bins,
    lognorm.cdf(bins, s=WSU_SIGMA_NORM, scale=np.exp(WSU_MU_NORM)),
    linewidth=2,
    label=r"WSU log-normal: $\mu={:0.2f}$; $\sigma={:0.2f}$".format(
        WSU_MU_NORM, WSU_SIGMA_NORM
    ),
)
ax.legend()

plt.savefig(CUSTOMER+"_Measured_Emissions_Cumulative.jpg", dpi=400, bbox_inches="tight")


## Assign Emission Factors

emission_sources = (
    emission_sources.assign(
        Bin=pd.cut(
            x=emission_sources.MeasuredSCFH,
            bins=[0, 0.1, 1, 10, np.inf],
            labels=["B-2", "B-1", "B0", "B1"],
        )
    )
    .assign(EmissionFactorInSCFH=lambda x: x.Bin.map(ITALGAS_EMISSION_FACTORS))
    .astype(
        {
            "Bin": str,
            "EmissionFactorInSCFH": float,
        }
    )
)


## Add Liters Per Minute Columns


emission_sources["EmissionFactorInLPM"] = (
    emission_sources["EmissionFactorInSCFH"] * SCFH_TO_SLPM_FACTOR
)





## Set True and False conditions on the priority score column
#(meeting on the 12/04/2022)
#- < 0.06 True
#- => 0.06 False


emission_sources["PriorityScore"] = emission_sources["PriorityScore"] < 0.06


## Customize Columns for Customer


emission_sources["MeasuredSLPM"] = (
    emission_sources["MeasuredSCFH"] * SCFH_TO_SLPM_FACTOR
)


emission_sources["EmissionFactorTimesLeakProbabilityLPM"] = (
    emission_sources["LeakProbability"] * emission_sources["EmissionFactorInLPM"]
)

emission_sources["ReportName"] = emission_sources["ReportId"].map(
    lambda x: "CR-" + x[:6].upper()
)

emission_sources["LISANumber"] = (
    emission_sources["ReportName"] + "-" + emission_sources["PeakNumber"].astype(str)
)


emission_sources["AssetLengthCoveredKM"] = (
    emission_sources["PipelineMeters"].astype(float)
    * emission_sources["AssetCoverageFrac"].astype(float)
    / 1000
)


COLUMN_RENAMES = {
    "ReportId": "pcubedreportguid",
    "ReportName": "pcubedreportname",
    "ReportTitle": "pcubedreportitle",
    "DateReportStarted": "pcubedreportdate",
    "PipelineMeters": "PipelineMeters".lower(),
    "AssetLengthCoveredKM": "km_in_fov",
    # "IsFiltered": "BelowRRA",
    "PriorityScore": "BelowRRA",
    "LeakProbability": "LeakProbability".lower(),
    "BoxId": "BoxId".lower(),
    # "LeakGrade": "LeakGrade".lower(),
    "codiceDispersione": "LeakGrade".lower(),
    # "FoundDateTime": "FoundDateTime".lower(),
    "dataLocalizzazione": "FoundDateTime".lower(),
    # "AG/BG": "agbg",
    "aereoInterrato": "agbg",
    "LeakFound": "LeakFound".lower(),
    # "LeakLocation": "LeakLocation".lower(),
    "indirizzoLocalizzazione": "LeakLocation".lower(),
    "LeakLatitude": "LeakLatitude".lower(),
    "LeakLongitude": "LeakLongitude".lower(),
    "MeasuredSCFH": "emissionrate_measured_scfh",
    "MeasuredSLPM": "emissionrate_measured_lpm",
    "Bin": "emission_bin",
    "EmissionFactorInLPM": "emission_factor_lpm",
    "EmissionFactorTimesLeakProbabilityLPM": "emissionfactor_leakprob_lpm",
    "City": "City".lower(),
    "Region": "Region".lower(),
}

emission_sources = emission_sources.rename(columns=COLUMN_RENAMES)


column_order = [
    "pcubedreportguid",
    "region",
    "city",
    "pcubedreportname",
    "pcubedreportitle",
    "pcubedreportdate",
    "PipelineMeters".lower(),
    "AssetCoverageFrac",
    "km_in_fov",
    "EmissionSourceId",
    "CH4",
    "MaxAmplitude",
    "EthaneRatio",
    "EthaneRatioUncertainty",
    "Disposition",
    "ClassificationConfidence",
    "LISANumber",
    "BelowRRA",
    "GpsLatitude",
    "GpsLongitude",
    "LeakProbability".lower(),
    "BoxId".lower(),
    "LeakGrade".lower(),
    "FoundDateTime".lower(),
    "agbg",
    "LeakFound".lower(),
    "LeakLocation".lower(),
    "LeakLatitude".lower(),
    "LeakLongitude".lower(),
    "emissionrate_measured_scfh",
    "emissionrate_measured_lpm",
    "emission_bin",
    "emission_factor_lpm",
    "emissionfactor_leakprob_lpm",
]

emission_sources = emission_sources[column_order]


# Add the region and city from Kibana if there are above RRA and from the PM sheet

## Load the PM smartsheet

smart_sheet = pd.read_excel(FINAL_REPORTS_PATH)


list_unique_reports = emission_sources["pcubedreportguid"].unique()
list_of_reports_with_only_below_RRA = []


for report in list_unique_reports:
    single_report = emission_sources.query("pcubedreportguid== @report")

    above_RRA_report = single_report.loc[(single_report["BelowRRA"] == False)]

    if (
        len(above_RRA_report) == 0
        or pd.isna(single_report["region"].iloc[0]) == True
        or pd.isna(single_report["city"].iloc[0]) == True
    ):
        #print("Report '{}' does not have above RRA lisas".format(report))
        list_of_reports_with_only_below_RRA.append(
            single_report["pcubedreportname"].iloc[0]
        )

    else:
        region_report = above_RRA_report["region"].iloc[0]
        city_report = above_RRA_report["city"].iloc[0]

        emission_sources.loc[
            emission_sources.pcubedreportguid == report, "region"
        ] = region_report
        emission_sources.loc[
            emission_sources.pcubedreportguid == report, "city"
        ] = city_report
        
        
for report_name in list_of_reports_with_only_below_RRA:

    single_report_smart_sheet = smart_sheet.query("Finalreportpcubed == @report_name")

    region_report = single_report_smart_sheet["Boundaryname"].iloc[0]
    #print(region_report)
    city_report = single_report_smart_sheet["Region"].iloc[0]

    emission_sources.loc[
        emission_sources.pcubedreportname == report_name, "region"
    ] = region_report
    emission_sources.loc[
        emission_sources.pcubedreportname == report_name, "city"
    ] = city_report
    
df_below_RRA = pd.DataFrame(
    list_of_reports_with_only_below_RRA, columns=["Finalreportpcubed"]
)
df_below_RRA.to_csv("data/reports_with_only_below_RRA.csv")



# Check the difference of 


# Get last report driving completion date

most_recent_date = pd.to_datetime(smart_sheet["Driving Completion Date"].max())
date_last_report = most_recent_date.strftime("%d-%m-%Y")


# Save for customer

emission_sources.to_csv(
    f"data/leaks-with-emission-factors-{CUSTOMER}_until_{date_last_report}.csv",
    index=False,
)
