import controllers.controller as control

process = control.ProcessSessionCleaner(max_age_hour=3)
process.execute()
