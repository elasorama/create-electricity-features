import logging
import azure.functions as func

# Importing the main function 
from aggregate_features import main

app = func.FunctionApp()

@app.schedule(schedule="0 */2 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timer_trigger_electricity(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    main(delta_hours=24)
    logging.info('Python timer trigger function executed.')
    