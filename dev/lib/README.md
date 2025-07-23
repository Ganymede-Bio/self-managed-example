# lib Folder

The contents of this folder will be available to each function.py file that is used in any Flow. 

For instance if your file is `lib/helpers.py` and has a function named `read_from_api` you would include the following in a `function.py` file

```python
from lib.helpers import read_from_api

def execute(
    df_sql_result: pd.DataFrame | list[pd.DataFrame],
    output_spreadsheet_name: str,
    ganymede_context: GanymedeContext,
) -> NodeReturn:
    
    read_from_api()

```