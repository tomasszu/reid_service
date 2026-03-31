# analysis/daylight_filter.py

from typing import List, Dict, Tuple
from datetime import datetime
import pytz

from astral import LocationInfo
from astral.sun import sun


class DaylightFilter:
    """
    Adds 'daytime': bool field to each sighting based on sunrise/sunset.
    Assumes all cameras are in the same city.
    """

    def __init__(self, latitude: float, longitude: float, timezone: str):
        self.latitude = latitude
        self.longitude = longitude
        self.timezone = timezone

        #Example
        # "Teika cams": {
        #     "latitude": 56.98,
        #     "longitude": 24.19,
        #     "timezone": "Europe/Riga"
        # }

        self.sun_cache: Dict[str, Tuple[datetime, datetime]] = {}

        self.location = LocationInfo(
            name="City",
            region="",
            timezone=timezone,
            latitude=latitude,
            longitude=longitude,
        )

    def _compute_sun_times(self, day: str) -> Tuple[datetime, datetime]:
        """
        Compute sunrise and sunset for given day.
        Cached per day.
        """
        if day in self.sun_cache:
            return self.sun_cache[day]

        tz = pytz.timezone(self.timezone)

        s = sun(self.location.observer, date=day, tzinfo=tz)

        sunrise = s["sunrise"]
        sunset = s["sunset"]

        self.sun_cache[day] = (sunrise, sunset)
        return sunrise, sunset

    # def mark_daytime(self, sightings: List[Sighting]) -> List[Sighting]:
    #     """
    #     Adds 'daytime': bool to each sighting.data
    #     """

    #     if not sightings:
    #         return sightings

    #     tz = pytz.timezone(self.timezone)

    #     for sighting in sightings:
    #         timestamp_ns = sighting.data["timestamp_ns"]
    #         day = sighting.day

    #         sunrise, sunset = self._compute_sun_times(day)

    #         timestamp_dt = datetime.fromtimestamp(
    #             timestamp_ns / 1e9,
    #             tz=tz
    #         )

    #         sighting.data["daytime"] = sunrise <= timestamp_dt <= sunset

    #     return sightings
    
    def is_daytime(self, timestamp_ns: int) -> bool:
        tz = pytz.timezone(self.timezone)

        timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1e9, tz=tz)
        day = timestamp_dt.date()

        sunrise, sunset = self._compute_sun_times(day)

        return sunrise <= timestamp_dt <= sunset