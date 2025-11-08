import copy
from io import BytesIO

import pandas as pd
from ganymede_sdk import GanymedeContext, Ganymede
from ganymede_sdk.io import NodeReturn

from lib.time_util import print_date_time

def execute(
    df_sql_result: pd.DataFrame | list[pd.DataFrame],
    output_spreadsheet_name: str,
    ganymede_context: GanymedeContext,
) -> NodeReturn:
    """
    Write table(s) from SQL query and stores Excel sheet in cloud storage.

    Parameters
    ----------
    df_sql_result : pd.DataFrame | list[pd.DataFrame], optional
        Table(s) or list of tables retrieved from user-defined SQL query
    output_spreadsheet_name : str
        Name of the output spreadsheet
    ganymede_context : GanymedeContext
        Ganymede context variable, which stores flow run metadata

    Returns
    -------
    NodeReturn
        Object containing data to store in data lake and/or file storage
    """
    print_date_time()
    g = Ganymede(ganymede_context)

    results = g.retrieve_tables("qPCR_Analysis_Calculated_Results")[
        "qPCR_Analysis_Calculated_Results"
    ]

    results = results.sort_values(["Well Column", "Well Row"])
    results.drop(columns=["Well Row"], inplace=True)
    bio = BytesIO()

    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        if isinstance(results, pd.DataFrame):
            for target_name in results["Gene"].unique():
                target_data = results[results["Gene"] == target_name]
                target_data.to_excel(writer, sheet_name=target_name, index=False)
        else:
            for idx, df in enumerate(results):
                df.to_excel(writer, sheet_name=str(idx), index=False)

    bio.seek(0)

    return NodeReturn(files_to_upload={"qPCR_analysis.xlsx": bio.read()})
