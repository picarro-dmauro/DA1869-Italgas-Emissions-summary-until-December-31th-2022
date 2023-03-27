import os
import warnings
import threading
import queue
from dotenv import load_dotenv
from collections import defaultdict
import matplotlib.pyplot as plt
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from sqlalchemy import create_engine
#from workercommon.dal.cursorfactory import CursorFactory
from apps_dal_dynamo import dynamodal

SCFH_TO_SLPM_FACTOR = 0.471947
P_CUBED_CRS = "EPSG:4326"

load_dotenv()
HOST = "20.80.80.81"
DATABASE = "SurveyorProductionLS"

USER = os.getenv("LOGSHIPPINGUSER")
PASSWORD = os.getenv("LOGSHIPPINGPW")
log_shipping_sql_alchemy_engine = create_engine(
    f"mssql+pyodbc://{USER}:{PASSWORD}@{HOST}:1433/{DATABASE}?"
    "driver=ODBC+Driver+17+for+SQL+Server"
)
# cursor_factory = CursorFactory(
#     server="20.80.80.81",
#     user=os.getenv("PCUBEDPRODUSER"),
#     password=os.getenv("PCUBEDPRODPW"),
#     database="SurveyorProductionLS",
#     tds_version="7.3",
# )



dynamo_dal = dynamodal.DynamoDalBase(
    dynamo_db_region='us-west-2',
    dynamo_db_access_key=os.environ.get("DYNAMOACCESSKEY"),
    dynamo_db_secret_key=os.environ.get("DYNAMOSECRETKEY"),
    dynamo_db_environment='SurveyorProduction',
    dynamo_db_local=False,
    dynamo_db_endpoint_url=None
)

elastic_credentials = [os.getenv("ELASTICUSER"), os.getenv("ELASTICPW")]


def query_mssql_iteratively(query_parameters, query_function,
                            sql_alchemy_engine=log_shipping_sql_alchemy_engine
                            ):
    """
    Queries database in cursor_factory n times, where n is the length of query_parameters,
    using the provided query_function. The query function returns a SQL query string with
    a given parameter embedded in it, as shown below.

    Example query function:

    def create_gap_query(report_id: str) -> str:
        query = "
        WITH SurveyTemp AS (SELECT SurveyId,
                                   ReportId
                            FROM ReportDrivingSurvey
                            WHERE ReportId = '{str(report_id)}')

        SELECT F.Shape.STAsText() AS Shape,
               S.ReportId,
               F.SurveyId
        FROM FieldOfView F
                 JOIN SurveyTemp S ON F.SurveyId = S.SurveyId
        "
        return query


    :param sql_alchemy_engine: e.g. create_engine(f"mssql+pyodbc://{USER}:{PASSWORD}@{HOST}:1433/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server")
    :param query_parameters: Iterable of elements to be embedded in SQL query string
    :param query_function: Creates the SQL query string with an element from query_parameters
    :return: Pandas data frame of the query results
    """
    result_list = []
    with sql_alchemy_engine.connect() as conn:
    #with cursor_factory.get_connection_pymssql() as conn:
        for query_parameter in query_parameters:
            query = query_function(query_parameter)
            result = pd.read_sql(sql=query, con=conn)
            result_list.append(result)
    return pd.concat(result_list, axis=0).reset_index(drop=True)


def create_survey_query(survey_id):
    query = f"""
    SELECT CONVERT(NVARCHAR(50), Id) AS SurveyId,
           CONVERT(NVARCHAR(50), AnalyzerId) AS AnalyzerId,
           StartEpoch,
           EndEpoch
    FROM Survey
    WHERE Id = '{survey_id}'      
    """
    return query


def create_peak_query(emission_source_id):
    query = f"""
    SELECT CONVERT(NVARCHAR(50), P.Id)                  AS PeakId,
           CONVERT(NVARCHAR(50), S.Id)                  AS SurveyId,
           CONVERT(NVARCHAR(50), S.AnalyzerId)          AS AnalyzerId,
           CONVERT(NVARCHAR(50), PESM.EmissionSourceId) AS EmissionSourceId,
           P.PlumeEpochStart,
           P.PlumeEpochEnd,
           P.LineIntegral,
           P.Distance,
           P.Sigma,
           P.EpochTime,
           P.Amplitude,
           P.CarSpeedEast,
           P.CarSpeedNorth,
           P.WindSpeedNorth,
           P.WindSpeedEast,
           P.WindDirectionStdDev,
           P.GpsLatitude,
           P.GpsLongitude,
           P.PlumeEmissionRate                          AS PCubedPlumeEmissionRate
    FROM PeakEmissionSourceMapping PESM
             JOIN Peak P ON PESM.PeakId = P.Id
             JOIN Survey S ON P.SurveyId = S.Id
    WHERE EmissionSourceId = '{emission_source_id}'
    """
    return query


def create_emission_source_query(report_prefix):
    query = f"""
    SELECT LOWER(CONVERT(NVARCHAR(50), ES.Id))                   AS EmissionSourceId,
           LOWER(CONVERT(NVARCHAR(50), ES.RepresentativePeakId)) AS RepresentativePeakId,
           LOWER(CONVERT(NVARCHAR(50), P.SurveyId))              AS SurveyId,
           LOWER(CONVERT(NVARCHAR(50), S.AnalyzerId))            AS AnalyzerId,
           LOWER(CONVERT(NVARCHAR(50), ES.ReportId))             AS ReportId,
           CONCAT('CR-', SUBSTRING(CONVERT(nvarchar(50), R.Id), 1, 6)) ReportName,
           R.ReportTitle,
           EADT.Name                                             AS DispositionName,
           EADT.Id                                               AS Disposition,
           ES.PeakNumber,
           ES.PriorityScore,
           ES.EmissionRate                                       AS MeasuredSCFH,
           ES.MaxAmplitude,
           ES.GpsLatitude,
           ES.GpsLongitude,
           ES.CH4,
           ES.EthaneRatio,
           ES.EthaneRatioUncertainty,
           ES.ClassificationConfidence,
           R.DateStarted                                         AS DateReportStarted,
           S.EndDateTime                                         AS SurveyEndDateTime,
           YEAR(S.EndDateTime)                                   AS Year,
           C.Name                                                AS Customer
    FROM EmissionSource ES
             JOIN Peak P ON ES.RepresentativePeakId = P.Id
             JOIN Survey S ON P.SurveyId = S.Id
             JOIN EthaneAnalysisDispositionTypes EADT ON ES.Disposition = EADT.Id
             JOIN Report R ON ES.ReportId = R.Id
             JOIN Customer C ON R.CustomerId = C.Id
             JOIN ReportAreaCovered RAC ON R.Id = RAC.ReportId
    WHERE ES.ReportId LIKE '{report_prefix}%'
    """
    return query

def create_emission_source_query_report_id(report_prefix):
    query = f"""
    SELECT LOWER(CONVERT(NVARCHAR(50), ES.Id))                   AS EmissionSourceId,
           LOWER(CONVERT(NVARCHAR(50), ES.RepresentativePeakId)) AS RepresentativePeakId,
           LOWER(CONVERT(NVARCHAR(50), P.SurveyId))              AS SurveyId,
           LOWER(CONVERT(NVARCHAR(50), S.AnalyzerId))            AS AnalyzerId,
           LOWER(CONVERT(NVARCHAR(50), ES.ReportId))             AS ReportId,
           YEAR(S.EndDateTime)                                   AS Year,
           ES.PeakNumber,
           C.Name                                                AS Customer
           
           
           
    FROM EmissionSource ES
             JOIN Peak P ON ES.RepresentativePeakId = P.Id
             JOIN Survey S ON P.SurveyId = S.Id
             JOIN EthaneAnalysisDispositionTypes EADT ON ES.Disposition = EADT.Id
             JOIN Report R ON ES.ReportId = R.Id
             JOIN Customer C ON R.CustomerId = C.Id
             JOIN ReportAreaCovered RAC ON R.Id = RAC.ReportId
    WHERE ES.ReportId LIKE '{report_prefix}%'
    """
    return query


def get_sample_of_peak_measurements_from_each_analyzer(emission_sources_df, sample_fraction=0.2):
    """
    This is the ugliest function I've ever written. Given the short expected life of this code,
    no refactoring is planned.
    """
    analyzer_measurement_samples_receptacle = defaultdict(list)
    print("getting a random fraction of peaks from each analyzer")
    fraction_of_emission_source_ids = emission_sources_df.groupby(
        "AnalyzerId").sample(frac=sample_fraction)["EmissionSourceId"].tolist()
    peaks = query_mssql_iteratively(fraction_of_emission_source_ids, create_peak_query)

    def get_analyzer_group_measurement_values(group):
        def get_measurement_values(peak):
            nonlocal analyzer_measurement_samples_receptacle
            peak_measurements = get_measurements(
                peak.AnalyzerId, peak.PlumeEpochStart, peak.PlumeEpochEnd
            )
            peak_measurements["PeakId"] = peak.PeakId
            analyzer_measurement_samples_receptacle[group.name].append(peak_measurements)

        group.apply(get_measurement_values, axis=1)

    print("getting measurement samples")
    _ = peaks.groupby("AnalyzerId").apply(get_analyzer_group_measurement_values)

    analyzer_measurement_samples = dict()
    for analyzer_id, list_of_dfs in analyzer_measurement_samples_receptacle.items():
        analyzer_measurement_samples[analyzer_id] = pd.concat(list_of_dfs)

    return analyzer_measurement_samples


def get_measurements(analyzer_id, start_epoch, end_epoch):
    measurements_dict = dynamo_dal.load_measurement_data(analyzer_id, start_epoch, end_epoch)
    measurements = pd.DataFrame(measurements_dict)
    return measurements


def get_elastic_index(index_name):
    es = es_conn_tlimit("20.80.30.39", "9200", elastic_credentials, 24 * 3600)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        query_results = dump_index(es, index_name, True)
    return pd.DataFrame(query_results)


def es_conn_tlimit(ip, port, elastic_creds, tlimit_s):
    # start a thread queue
    qu = queue.Queue()
    # create the threading process
    es_conn_thread = threading.Thread(target=connect_to_es,
                                      name="es_conn_thread",
                                      args=[ip, port, elastic_creds, qu], )
    # start the thread
    es_conn_thread.start()
    # let the thread run for some number of seconds
    es_conn_thread.join(int(tlimit_s))
    # get the result put in the queue
    es = qu.get()
    return es


def connect_to_es(ip, port, elastic_creds, qu):
    
    
    # Connect to the elastic cluster
    es = Elasticsearch(['https://%s:%s' % (ip, port)],
                       http_auth=(elastic_creds[0], elastic_creds[1]),
                       verify_certs=False, timeout=60, max_retries=10, retry_on_timeout=True)
    # put the result in the thread queue
    qu.put(es)
    return


def dump_index(es_obj, index_name, include_id_flag):
    # dump the contents of an entire index to a list in python, works on indices with any number of documents with
    # a scroll query
    query_body = {
        "query": {
            "match_all": {}
        }
    }

    return scroll_query(es_obj, index_name, query_body, include_id=include_id_flag)


def scroll_query(es_obj, index_name, query_body, include_id):
    # use the scan helper to intitiate the scroll query, this eliminates problems with having too many active scrolls
    # preserve_order True makes the query inefficient, however, there is a bug right now with setting it to false
    #   tracking this issue in https://github.com/elastic/elasticsearch-py/issues/931
    res = scan(es_obj, query=query_body, index=index_name, preserve_order=True)

    query_results = []  # initialize the results list
    # iterate through the scan results
    for doc in res:
        query_results.append(doc['_source'])
        if include_id:
            query_results[-1]['_id'] = doc['_id']

    return query_results


def plot_histogram(
        x,
        title,
        x_label=None,
        y_label="density",
        n_bins="doane",
        label=None,
        bar_colors=None,
        log=False,
        density=True,
        fig_size=(12, 8)
):
    fig, ax = plt.subplots(figsize=fig_size)

    _, bins, _ = ax.hist(
        x, bins=n_bins, density=density, log=log, label=label, color=bar_colors,
        histtype="stepfilled", alpha=0.3,
        linewidth=2, align="mid"
    )

    if label:
        ax.legend(loc="best", fontsize=15, frameon=False)

    format_ax(ax, title, x_label=x_label, y_label=y_label)
    return ax, bins


def plot_cdf(
        x,
        title,
        x_label=None,
        y_label="cumulative probability",
        n_bins="sqrt",
        log_x=False,
        log_y=False,
        label=None,
        color=None,
):
    fig, ax = plt.subplots(figsize=(12, 8))
    _, bins, _ = ax.hist(
        x,
        n_bins,
        density=True,
        histtype="stepfilled",
        cumulative=True,
        label=label,
        color=color,
        linewidth=2,
        alpha=0.3
    )

    if log_x:
        ax.set_xscale('log')
    if log_y:
        ax.set_yscale('log')

    ax.grid(which="both", alpha=0.3)

    format_ax(ax, title, x_label=x_label, y_label=y_label)
    return ax, bins


def format_ax(ax, title, x_label, y_label, title_size=20, label_size=15):
    ax.set_title(title, fontsize=title_size)
    ax.set_ylabel(y_label, fontsize=label_size)
    ax.set_xlabel(x_label, fontsize=label_size)
    ax.tick_params(axis="both", which="major", labelsize=label_size)
    ax.tick_params(axis="both", which="minor", labelsize=label_size)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    return ax




### WINTERS QUERIES ####