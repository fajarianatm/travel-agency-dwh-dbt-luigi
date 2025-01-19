import luigi
import pandas as pd
import os

from pipeline.extract import Extract
from pipeline.load import Load
from pipeline.transform import Transform
from pipeline.utils.concat_dataframe import concat_dataframes
from pipeline.utils.copy_log import copy_log
from pipeline.utils.delete_temp_data import delete_temp

from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Read env variables
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_LOG = os.getenv("DIR_LOG")

# # Track the error using sentry
# sentry_sdk.init(
#     dsn = f"{SENTRY_DSN}"
# )


# Run the ELT pipeline
if __name__ == '__main__':
    luigi.build([Load(),
                 Extract(),
                 Transform()])
    
    # Create initial pipeline summary from extract summary
    extract_summary = pd.read_csv(f'{DIR_TEMP_DATA}/extract-summary.csv')
    extract_summary.to_csv(f'{DIR_ROOT_PROJECT}/pipeline_summary.csv', index=False)
    
    # Concat temp load summary to final summary
    concat_dataframes(
        df1 = pd.read_csv(f'{DIR_ROOT_PROJECT}/pipeline_summary.csv'),
        df2 = pd.read_csv(f'{DIR_TEMP_DATA}/load-summary.csv')
    )
    
    # Concat temp load summary to final summary
    concat_dataframes(
        df1 = pd.read_csv(f'{DIR_ROOT_PROJECT}/pipeline_summary.csv'),
        df2 = pd.read_csv(f'{DIR_TEMP_DATA}/transform-summary.csv')
    )
    
    # Append log from temp to final log
    copy_log(
        source_file = f'{DIR_TEMP_LOG}/logs.log',
        destination_file = f'{DIR_LOG}/logs.log'
    )
    
    # Delete temp data
    delete_temp(
        directory = f'{DIR_TEMP_DATA}'
    )
    
    # Delete temp log
    delete_temp(
        directory = f'{DIR_TEMP_LOG}'
    )