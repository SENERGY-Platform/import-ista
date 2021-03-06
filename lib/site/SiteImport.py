#  Copyright 2022 InfAI (CC SES)
#  #
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#  #
#       http://www.apache.org/licenses/LICENSE-2.0
#  #
#    Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import datetime
import json
import sched
import typing

import requests

from import_lib.import_lib import ImportLib, get_logger

from lib.site import Point

logger = get_logger(__name__)
baseUrl = 'https://api.prod.eed.ista.com/'
dtFormat = '%Y-%m-%d %H:%M:%S'


class SiteImport:
    def __init__(self, lib: ImportLib, scheduler: sched.scheduler):
        self.__lib = lib
        self.__scheduler = scheduler

        self.__email = self.__lib.get_config("EMAIL", None)
        if self.__email is None or len(self.__email) == 0:
            raise AssertionError("EMAIL not set")

        self.__pw = self.__lib.get_config("PW", None)
        if self.__pw is None or len(self.__pw) == 0:
            raise AssertionError("PW not set")

        self.__accessToken = None
        self.__refreshToken = None

        self.__uuid = self.__lib.get_config("UUID", None)
        if self.__uuid is None or len(self.__uuid) == 0:
            raise AssertionError("UUID not set")
        import pytz
        self.__timezone = pytz.timezone(self.__lib.get_config("TIMEZONE", 'Europe/Berlin'))
        self.__delay = (60 * 60 * 24) * self.__lib.get_config("EVERY_DAYS", 30)
        self.__last_dt, _ = self.__lib.get_last_published_datetime()
        self.__scheduler.enter(0, 1, self.__import)
        if self.__accessToken is None or len(self.__accessToken) == 0:
            try:
                data = {'email': self.__email, 'fromMobileApp': False, 'password': self.__pw}
                headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
                resp = requests.post(f"{baseUrl}login", data=json.dumps(data), headers= headers)
                if not resp.ok:
                    raise Exception("Request got unexpected status code " + str(resp.status_code))
                resp = resp.json()
                self.__accessToken = resp["accessToken"]
                self.__refreshToken = resp["refreshToken"]
            except Exception as e:
                logger.error(f"Could not login {e}")
                return

    def __import(self):
        try:
            resp = requests.get(
                f"{baseUrl}consumptions?consumptionUnitUuid={self.__uuid}", headers={"Authorization": self.__accessToken})
            if not resp.ok:
                raise Exception("Request got unexpected status code " + str(resp.status_code))
            resp = resp.json()
            points = self.__extract(resp)
            for dt, val in points:
                self.__lib.put(dt, val)
            logger.info(f"Imported {len(points)} most recent data points")
            self.__last_dt = points[len(points) - 1][0]

        except Exception as e:
            logger.error(f"Could not get data {e}")
            return
        finally:
            self.__scheduler.enter(self.__delay, 1, self.__import)

    def __extract(self, raw: typing.Dict) -> typing.List[typing.Tuple[datetime.datetime, typing.Dict]]:
        resp = []
        cost = {}
        for consumption in raw['consumptions']:
            for costs in raw['costs']:
                if consumption["date"]["month"] == costs["date"]["month"] and consumption["date"]["year"] == costs["date"]["year"]:
                    cost = costs
            point = Point.get_message(consumption, cost)
            date = datetime.datetime(consumption["date"]["year"], consumption["date"]["month"], 28)
            next_month = date + datetime.timedelta(days=4)
            last_day_of_month = next_month - datetime.timedelta(days=next_month.day)
            date = datetime.datetime(consumption["date"]["year"], consumption["date"]["month"], last_day_of_month.day)
            resp.append((date, point))
        return resp
