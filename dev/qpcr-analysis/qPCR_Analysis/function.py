import copy
from io import BytesIO
from lib.time_util import print_date_time

import pandas as pd
from ganymede_sdk import GanymedeContext, Ganymede
from ganymede_sdk.io import NodeReturn


def execute(
    df_sql_result: pd.DataFrame | list[pd.DataFrame], ganymede_context: GanymedeContext
) -> NodeReturn:
    """
    Process tabular data from user-defined SQL query, writing results back to data lake.  Data
    is written to the output bucket.

    Parameters
    ----------
    df_sql_result : pd.DataFrame | list[pd.DataFrame]
        Table(s) or list of tables retrieved from user-defined SQL query
    ganymede_context : GanymedeContext
        Ganymede context variable, which stores flow run metadata

    Returns
    -------
    NodeReturn
        Object containing data to store in data lake and/or file storage.  NodeReturn object takes
        2 parameters:
        - tables_to_upload: dict[str, pd.DataFrame]
            keys are table names, values are pandas DataFrames to upload
        - files_to_upload: dict[str, bytes]
            keys are file names, values are file data to upload

    Notes
    -----
    Files can also be retrieved and processed using the list_files and retrieve_files functions.
    Documentation on these functions can be found at https://docs.ganymede.bio/sdk/GanymedeClass
    """
    print_date_time()
    g = Ganymede(ganymede_context)
    tables = g.retrieve_tables(["qPCR_Analysis_Results"])
    results_data = tables["qPCR_Analysis_Results"]

    results_data["Well Row"] = [
        "".join([i for i in x if i.isalpha()]) for x in results_data["Well"]
    ]

    results_data["Well Column"] = [
        "".join([i for i in x if i.isdigit()]) for x in results_data["Well"]
    ]
    results_data["Well Column"] = results_data["Well Column"].astype(int)

    housekeeping_gene = "Gapdh"
    control_condition = "PBS"

    housekeeping = (
        results_data[results_data["Gene"] == housekeeping_gene]
        .groupby(["Condition", "Time Point"])["Ct Value"]
        .mean()
        .reset_index()
    )
    housekeeping = housekeeping.rename(columns={"Ct Value": "Ct_HK"})

    results_data = results_data.merge(housekeeping, on=["Condition", "Time Point"])

    calculated_av_ct = (
        results_data.groupby(["Condition", "Gene", "Time Point"])["Ct Value"].mean().reset_index()
    )

    results_data["av Ct"] = results_data.apply(
        lambda row: calculated_av_ct[
            (calculated_av_ct["Gene"] == row["Gene"])
            & (calculated_av_ct["Condition"] == row["Condition"])
            & (calculated_av_ct["Time Point"] == row["Time Point"])
        ]
        .reset_index()
        .loc[0]["Ct Value"],
        axis=1,
    )

    results_data["delta Ct"] = results_data["av Ct"] - results_data["Ct_HK"]

    control_ct = (
        results_data[results_data["Condition"] == control_condition]
        .groupby(["Time Point", "Gene"])["delta Ct"]
        .mean()
        .reset_index()
    )
    control_ct = control_ct.rename(columns={"delta Ct": "delta Ct Control"})

    # Merge with dataframe to calculate ΔΔCt
    results_data = results_data.merge(control_ct, on=["Time Point", "Gene"])
    results_data["delta delta Ct"] = results_data["delta Ct"] - results_data["delta Ct Control"]

    results_data["Fold Induction"] = 2 ** (-results_data["delta delta Ct"])
    results_data["Condition & Gene"] = results_data["Condition"] + "-" + results_data["Gene"]

    # final_data = pd.DataFrame(final_data)

    return NodeReturn(tables_to_upload={"qPCR_Analysis_Calculated_Results": results_data})
