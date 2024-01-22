import logging
import azure.functions as func

# Importing the main functions
from aggregate_features import main as aggregate_features
from aggregate_to_timeseries import main as aggregate_to_timeseries
from aggregate_to_power_consumption import main as aggregate_to_power_consumption
from create_analysis_data import main as create_analysis_data

app = func.FunctionApp()

@app.schedule(schedule="0 */2 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timer_trigger_electricity(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    aggregate_features(delta_hours=24)
    aggregate_to_timeseries()
    aggregate_to_power_consumption()
    create_analysis_data()
    logging.info('Python timer trigger function executed.')
    