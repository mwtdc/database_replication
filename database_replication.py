#!/usr/bin/python3.9
#!/usr/bin/env python
# coding: utf-8

import datetime
import logging
import pathlib
import urllib.parse
import warnings
from sys import platform

import pandas as pd
import requests
import yaml
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from sqlalchemy import create_engine

start_time = datetime.datetime.now()
warnings.filterwarnings("ignore")

print("Копирование между базами запущено.", datetime.datetime.now())

# Общий раздел

# Настройки для логера
if platform == "linux" or platform == "linux2":
    logging.basicConfig(
        filename="/var/log/log-execute/database_replication.log.txt",
        level=logging.INFO,
        format=(
            "%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d -"
            " %(message)s"
        ),
    )
elif platform == "win32":
    logging.basicConfig(
        filename=(
            f"{pathlib.Path(__file__).parent.absolute()}"
            "/database_replication.log.txt"
        ),
        level=logging.INFO,
        format=(
            "%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d -"
            " %(message)s"
        ),
    )


# Загружаем yaml файл с настройками
with open(
    f"{pathlib.Path(__file__).parent.absolute()}/settings.yaml", "r"
) as yaml_file:
    settings = yaml.safe_load(yaml_file)
telegram_settings = pd.DataFrame(settings["telegram"])
sql_settings = pd.DataFrame(settings["sql_db"])


# Функция отправки уведомлений в telegram на любое количество каналов
# (указать данные в yaml файле настроек)
def telegram(i, text):
    try:
        msg = urllib.parse.quote(str(text))
        bot_token = str(telegram_settings.bot_token[i])
        channel_id = str(telegram_settings.channel_id[i])

        retry_strategy = Retry(
            total=3,
            status_forcelist=[101, 429, 500, 502, 503, 504],
            method_whitelist=["GET", "POST"],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)

        http.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={channel_id}&text={msg}",
            verify=False,
            timeout=10,
        )
    except Exception as err:
        print(f"database_replication: Ошибка при отправке в telegram -  {err}")
        logging.error(
            f"database_replication: Ошибка при отправке в telegram -  {err}"
        )


# Функция коннекта к базе Mysql
# (для выбора базы задать порядковый номер числом !!! начинается с 0 !!!!!)
def connection(i):
    host_yaml = str(sql_settings.host[i])
    user_yaml = str(sql_settings.user[i])
    port_yaml = int(sql_settings.port[i])
    password_yaml = str(sql_settings.password[i])
    database_yaml = str(sql_settings.database[i])
    db_data = f"mysql://{user_yaml}:{password_yaml}@{host_yaml}:{port_yaml}/{database_yaml}"
    return create_engine(db_data).connect()


def load_data_from_db(
    db_name,
    col_from_database,
    connect_id,
    date_from,
    date_to,
    condition_column,
):
    telegram(1, "database_replication: Старт загрузки из БД.")
    logging.info("database_replication: Старт загрузки из БД.")

    list_col_database = ",".join(col_from_database)
    connection_db = connection(connect_id)
    query = (
        f"select {list_col_database} from {db_name} where"
        f" {condition_column} >= '{date_from}' and {condition_column} <"
        f" '{date_to}';"
    )
    weather_dataframe = pd.read_sql(sql=query, con=connection_db)

    telegram(1, "database_replication: Финиш загрузки из БД.")
    logging.info("database_replication: Финиш загрузки из БД.")
    return weather_dataframe


def load_data_to_db(db_name, connect_id, weather_dataframe):
    telegram(1, "database_replication: Старт записи в БД.")
    logging.info("database_replication: Старт записи в БД.")

    weather_dataframe = pd.DataFrame(weather_dataframe)
    connection_om = connection(connect_id)
    weather_dataframe.to_sql(
        name=db_name,
        con=connection_om,
        if_exists="append",
        index=False,
        chunksize=5000,
    )
    telegram(1, "database_replication: Финиш записи из БД.")
    logging.info("database_replication: Финиш записи из БД.")


def clear_pbr_br_grafana(
    connect_id,
):
    telegram(1, "database_replication: Старт очистки БД.")
    logging.info("database_replication: Старт очистки БД.")

    connection_db = connection(connect_id)
    query = (
        "DELETE FROM pbr_br_grafana.pbr_br WHERE dt < SUBDATE(CURDATE(), 7);"
    )
    connection_db.execute(query)
    telegram(1, "database_replication: Финиш очистки БД.")
    logging.info("database_replication: Финиш очистки БД.")


OPENMETEO_COLUMNS_DB = [
    "gtp",
    "datetime_msc",
    "loadtime",
    "temperature_2m",
    "relativehumidity_2m",
    "dewpoint_2m",
    "apparent_temperature",
    "pressure_msl",
    "surface_pressure",
    "cloudcover",
    "cloudcover_low",
    "cloudcover_mid",
    "cloudcover_high",
    "windspeed_10m",
    "windspeed_80m",
    "windspeed_120m",
    "windspeed_180m",
    "winddirection_10m",
    "winddirection_80m",
    "winddirection_120m",
    "winddirection_180m",
    "windgusts_10m",
    "shortwave_radiation",
    "direct_radiation",
    "direct_normal_irradiance",
    "diffuse_radiation",
    "vapor_pressure_deficit",
    "evapotranspiration",
    "et0_fao_evapotranspiration",
    "precipitation",
    "snowfall",
    "rain",
    "showers",
    "weathercode",
    "snow_depth",
    "freezinglevel_height",
    "soil_temperature_0cm",
    "soil_temperature_6cm",
    "soil_temperature_18cm",
    "soil_temperature_54cm",
    "soil_moisture_0_1cm",
    "soil_moisture_1_3cm",
    "soil_moisture_3_9cm",
    "soil_moisture_9_27cm",
    "soil_moisture_27_81cm",
]

VISUALCROSSING_COLUMNS_DB = [
    "gtp",
    "datetime",
    "tzoffset",
    "datetime_msc",
    "loadtime",
    "sunrise",
    "sunset",
    "temp",
    "dew",
    "humidity",
    "precip",
    "precipprob",
    "preciptype",
    "snow",
    "snowdepth",
    "windgust",
    "windspeed",
    "pressure",
    "cloudcover",
    "visibility",
    "solarradiation",
    "solarenergy",
    "uvindex",
    "severerisk",
    "conditions",
]

TOMORROW_IO_COLUMNS_DB = [
    "gtp",
    "datetime_msc",
    "loadtime",
    "temperature",
    "temperatureApparent",
    "dewPoint",
    "humidity",
    "windSpeed",
    "windDirection",
    "windGust",
    "pressureSurfaceLevel",
    "precipitationIntensity",
    "rainIntensity",
    "freezingRainIntensity",
    "snowIntensity",
    "sleetIntensity",
    "precipitationProbability",
    "precipitationType",
    "rainAccumulation",
    "snowAccumulation",
    "snowAccumulationLwe",
    "sleetAccumulation",
    "sleetAccumulationLwe",
    "iceAccumulation",
    "iceAccumulationLwe",
    "visibility",
    "cloudCover",
    "cloudBase",
    "cloudCeiling",
    "uvIndex",
    "evapotranspiration",
    "weatherCode",
]

PBR_BR_COLUMNS_DB = [
    "GTP_ID",
    "GTP_NAME",
    "dt",
    "SESSION_DATE",
    "SESSION_NUMBER",
    "SESSION_INTERVAL",
    "TG",
    "PminPDG",
    "PmaxPDG",
    "PVsvgo",
    "PminVsvgo",
    "PmaxVsvgo",
    "PminBR",
    "PmaxBR",
    "IBR",
    "CbUP",
    "CbDown",
    "CRSV",
    "TotalBR",
    "EVR",
    "OCPU",
    "OCPS",
    "Pmin",
    "Pmax",
]


DATE_FROM = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime(
    "%Y-%m-%d"
)
DATE_TO = (datetime.datetime.today() + datetime.timedelta(days=0)).strftime(
    "%Y-%m-%d"
)


# 0 - connection 237 база с ск
# 1 - connection 42 база трд
# 2 - connection 237 база pbr_br с ск
# 3 - connection 42 база pbr_br трд
# 4 - connection 237 база pbr_br_grafana с ск
# 5 - connection 42 база pbr_br_grafana трд
if __name__ == "__main__":
    # visualcrossing

    weather_dataframe = load_data_from_db(
        "forecast",
        VISUALCROSSING_COLUMNS_DB,
        0,
        DATE_FROM,
        DATE_TO,
        "loadtime",
    )
    load_data_to_db(
        "forecast",
        1,
        weather_dataframe,
    )

    # openmeteo

    weather_dataframe = load_data_from_db(
        "openmeteo", OPENMETEO_COLUMNS_DB, 0, DATE_FROM, DATE_TO, "loadtime"
    )
    load_data_to_db(
        "openmeteo",
        1,
        weather_dataframe,
    )

    # tomorrow_io

    weather_dataframe = load_data_from_db(
        "tomorrow_io",
        TOMORROW_IO_COLUMNS_DB,
        0,
        DATE_FROM,
        DATE_TO,
        "loadtime",
    )
    load_data_to_db(
        "tomorrow_io",
        1,
        weather_dataframe,
    )

    # pbr_br

    weather_dataframe = load_data_from_db(
        "pbr_br", PBR_BR_COLUMNS_DB, 2, DATE_FROM, DATE_TO, "dt"
    )
    load_data_to_db(
        "pbr_br",
        3,
        weather_dataframe,
    )

    # pbr_br_grafana

    weather_dataframe = load_data_from_db(
        "pbr_br", PBR_BR_COLUMNS_DB, 4, DATE_FROM, DATE_TO, "dt"
    )
    load_data_to_db(
        "pbr_br",
        5,
        weather_dataframe,
    )
    clear_pbr_br_grafana(5)
    logging.info("database_replication: Репликация БД завершена.")
    telegram(0, "database_replication: Репликация БД завершена.")


print("Время выполнения:", datetime.datetime.now() - start_time)
