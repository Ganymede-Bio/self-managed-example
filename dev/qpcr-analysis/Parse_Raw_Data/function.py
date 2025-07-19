from io import BytesIO

import pandas as pd
from ganymede_sdk import GanymedeContext
from ganymede_sdk.io import NodeReturn


def execute(excel_file: bytes | dict[str, bytes], ganymede_context: GanymedeContext) -> NodeReturn:
    """
    Reads Excel file and stores processed table(s) in data lake.

    Parameters
    ----------
    excel_file : bytes | dict[str, bytes]
        Excel file as a bytes object or as dict indexed by filename
    ganymede_context : GanymedeContext
        Ganymede context variable, which stores flow run metadata

    Returns
    -------
    NodeReturn
        Object containing data to store in data lake and/or file storage

    Notes
    -----
    Excel_file is represented in bytes so user can handle cases where Excel spreadsheet is
    a binary object within this function
    """

    # passing sheet_name=None returns a dict of dataframes with sheet names as keys
    if isinstance(excel_file, dict):
        first_excel_file = list(excel_file.values()).pop()
        # plate_map = pd.read_excel(BytesIO(first_excel_file), sheet_name="Sample Setup", skiprows=40)
        # raw_data = pd.read_excel(BytesIO(first_excel_file), sheet_name="Raw Data", skiprows=40)
        # amplification_data = pd.read_excel(
        #     BytesIO(first_excel_file), sheet_name="Amplification Data", skiprows=40
        # )
        # results_data = pd.read_excel(
        #     BytesIO(first_excel_file), sheet_name="Results", skiprows=40, nrows=488
        # )
        results_data = pd.read_excel(BytesIO(first_excel_file), skiprows=19)
        metadata = pd.read_excel(
            BytesIO(first_excel_file),
            # sheet_name="Sample Setup",
            nrows=17,
            names=["Variable", "Value"],
            usecols=[0, 1],
        )
    else:
        # plate_map = pd.read_excel(BytesIO(excel_file), sheet_name="Sample Setup", skiprows=40)
        # raw_data = pd.read_excel(BytesIO(excel_file), sheet_name="Raw Data", skiprows=40)
        # amplification_data = pd.read_excel(
        #     BytesIO(excel_file), sheet_name="Amplification Data", skiprows=40
        # )
        # results_data = pd.read_excel(
        #     BytesIO(excel_file), sheet_name="Results", skiprows=40, nrows=488
        # )
        results_data = pd.read_excel(BytesIO(excel_file), skiprows=19)
        metadata = pd.read_excel(
            BytesIO(excel_file),
            # sheet_name="Sample Setup",
            nrows=17,
            names=["Variable", "Value"],
            usecols=[0, 1],
        )

    tables_to_upload = {
        # "qPCR_Analysis_Sample_Setup": plate_map,
        # "qPCR_Analysis_Raw_Data": raw_data,
        # "qPCR_Analysis_Amplification_Data": amplification_data,
        "qPCR_Analysis_Results": results_data,
        "qPCR_Analysis_Metadata": metadata,
    }

    return NodeReturn(tables_to_upload=tables_to_upload)