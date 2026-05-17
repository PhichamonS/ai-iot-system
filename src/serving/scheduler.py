from apscheduler.schedulers.blocking import BlockingScheduler
from src.training.train_sarima  import train_all_meters
from src.training.train_xgboost import train_xgboost
from src.serving.predictor      import run_all_forecasts
from src.evaluation.evaluate    import compare_models

scheduler = BlockingScheduler()

# Retrain SARIMA daily at 01:00
@scheduler.scheduled_job('cron', hour=1, minute=0)
def daily_sarima_retrain():
    print("=== Daily SARIMA retrain ===")
    train_all_meters()
    run_all_forecasts(model_type="sarima")
    compare_models()

# Retrain XGBoost weekly on Monday at 02:00
@scheduler.scheduled_job('cron', day_of_week='mon', hour=2, minute=0)
def weekly_xgboost_retrain():
    print("=== Weekly XGBoost retrain ===")
    train_xgboost(days=90)
    run_all_forecasts(model_type="xgboost")

# Run forecast every 15 minutes
@scheduler.scheduled_job('interval', minutes=15)
def forecast_tick():
    print("=== Forecast tick ===")
    run_all_forecasts(model_type="sarima")

if __name__ == "__main__":
    print("Scheduler started...")
    scheduler.start()