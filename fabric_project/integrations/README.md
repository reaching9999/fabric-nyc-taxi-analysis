# Integrations

External tool integrations for the fabric project.

## Setup

Run `pip install -r requirements.txt` to install dependancies.

## Task 1: Weather + Grafana

Fetches live weather data for NYC and writes to InfluxDB. Can be visualised in Grafana.

- Edit `weather_etl.py` with your influx credentials
- Run: `python weather_etl.py`
- Check influxdb dashboard to see data comming in

## Task 2: Telegram Bot + Great Expectations

Quality checks on taxi data, results sent via telegram bot.

- Create bot with @BotFather on telegram and get token
- Edit `telegram_data_guard.py` with your token
- Run: `python telegram_data_guard.py`
- Send /check to your bot

## Task 3: Dropbox + Power Automate

Uploads daily summary JSON to dropbox, triggers power automate flow.

- Get dropbox access token from app console
- Edit `upload_json.py` with token
- Run: `python upload_json.py`
- Power automate picks up the file and sends email notificaiton
