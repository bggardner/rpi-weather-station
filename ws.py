import datetime
import math
import threading
import time

import bme280
from gpiozero import Button, MCP3008
import numpy
import pytz
import smbus2


class IntervalTimer(threading.Thread):
    """Call a function every specified number of seconds:
            t = IntervalTimer(30.0, function, args=None, kwargs=None)
            t.start()
            t.cancel()    # stop the timer's action if it's still running
    """

    def __init__(self, interval, function, args=None, kwargs=None):
        super().__init__(args=args, kwargs=kwargs, daemon=True)
        self.interval = interval
        self.function = function
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.finished = threading.Event()

    def cancel(self):
        self.finished.set()

    def run(self):
        next_run = time.time() + self.interval
        while not self.finished.wait(next_run - time.time()):
            if self.finished.is_set():
                break
            threading.Thread(target=self.function, args=self.args, kwargs=self.kwargs, daemon=True).start()
            next_run += self.interval


class IntervalSampler:

    def __init__(self, interval: datetime.timedelta, duration: datetime.timedelta, callback=None):
        self._interval = interval.total_seconds()
        self._size = int(duration.total_seconds() / interval.total_seconds()) # Could use math.ceil() instead
        self._callback = callback

    def _sample(self):
        raise NotImplementedError

    def _store(self, sample):
        self._data.append(sample)
        self._data = self._data[-self._size:]
        if self._callback is not None:
            threading.Thread(target=self._callback, args=(self._data,)).start()

    @property
    def duration(self):
        return self._interval * self._size

    @property
    def interval(self):
        return self._interval

    @property
    def size(self):
        return self._size

    def start(self):
        self._data = []
        threading.Thread(target=self._sample).start()
        self._timer = IntervalTimer(self._interval, self._sample).start()

    def cancel(self):
        self._timer.cancel()


class ButtonSampler(IntervalSampler):

    def __init__(self, gpio: int, interval: datetime.timedelta, duration: datetime.timedelta, callback=None):
        super().__init__(interval, duration, callback)
        self._button = Button(gpio) # when_pressed is not called if Button is not "saved" as an attribute
        self._button.when_pressed = self._tick

    def _sample(self):
        self._store(self._count)
        self._count = 0

    def cancel(self):
        self._button.when_pressed = None
        super().cancel()

    @property
    def gpio(self):
       return self._gpio

    def start(self):
        self._count = 0
        self._button.when_pressed = self._tick
        super().start()

    def _tick(self):
        self._count += 1


class MCP3008Sampler(IntervalSampler):

    def __init__(self, channel: int, interval: datetime.timedelta, duration: datetime.timedelta, callback=None):
        super().__init__(interval, duration, callback)
        self._adc = MCP3008(channel)

    def _sample(self):
        self._store(self._adc.value)

    @property
    def channel(self):
       return self._channel


class WindDirectionSampler(MCP3008Sampler):

    RESISTANCE_BY_ANGLE = { # Sorted by decreasing resistance
        270: 120000,
        315: 64900,
        292.5: 42120,
        0: 33000,
        337.5: 21880,
        225: 16000,
        247.5: 14120,
        45: 8200,
        22.5: 6570,
        180: 3900,
        202.5: 3140,
        135: 2200,
        157.5: 1410,
        90: 1000,
        67.5: 891,
        112.5: 668
    }

    def __init__(self, r: float, channel: int, interval: datetime.timedelta, duration: datetime.timedelta, callback=None):
        super().__init__(channel, interval, duration, callback)
        values = []
        for resistance in self.RESISTANCE_BY_ANGLE.values():
            values.append(r / (r + resistance))
        self._thresholds_by_angle = {}
        i = 0
        for angle in self.RESISTANCE_BY_ANGLE.keys():
            if i == len(self.RESISTANCE_BY_ANGLE) - 1:
                self._thresholds_by_angle.update({angle: 1})
                continue
            self._thresholds_by_angle.update({angle: (values[i] + values[i + 1]) / 2})
            i += 1

    def _sample(self):
        self._store(self.degree)

    @property
    def degree(self):
        value = self._adc.value
        for angle, threshold in self._thresholds_by_angle.items():
            if value <= threshold:
                return angle


class BME280Sampler(IntervalSampler):

    I2C_ADDRESS = 0x77

    def __init__(self, i2c_bus: int, interval: datetime.timedelta, duration: datetime.timedelta, callback=None):
        super().__init__(interval, duration, callback)
        self._bus = smbus2.SMBus(i2c_bus)
        self._calibration_params = bme280.load_calibration_params(self._bus, self.I2C_ADDRESS)
        pass # Capture trends with m = numpy.polyfit(numpy.array(range(1, len(y))) * self._interval, y, 1)[0]

    def _sample(self):
        self._store(bme280.sample(self._bus, self.I2C_ADDRESS, self._calibration_params))

    @property
    def bus(self):
        return self._bus


class SensorReading:

    def __init__(self, value=None, timestamp=None, unit=None):
        self.value = value
        self.timestamp = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC) if timestamp is None else timestamp
        self.unit = unit

    def update(self, value, timestamp=None):
        self.value = value
        self.timestamp = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC) if timestamp is None else timestamp


class WeatherStation:

    # Busses, GPIOs, and ADC channel selection
    BME280_I2C_BUS = 1
    RAIN_SENSOR_GPIO = 6
    WIND_SPEED_SENSOR_GPIO = 5
    WIND_DIRECTION_RESISTOR = 4700
    WIND_DIRECTION_ADC_CHANNEL = 0 # MCP3008

    # Sample intervals and total durations
    AIR_SAMPLE_INTERVAL = datetime.timedelta(minutes=1)
    AIR_SAMPLE_DURATION = datetime.timedelta(hours=3) # per NOAA for reporting changes
    RAIN_SAMPLE_INTERVAL = datetime.timedelta(minutes=5)
    RAIN_SAMPLE_DURATION = datetime.timedelta(days=1)
    WIND_SPEED_SAMPLE_INTERVAL = datetime.timedelta(seconds=3) # per WMO for gusts
    WIND_SPEED_SAMPLE_DURATION = datetime.timedelta(minutes=10) # Keep 10 minutes of data
    WIND_DIRECTION_SAMPLE_INTERVAL = datetime.timedelta(seconds=1) # Direction should be sampled at least as much as speed
    WIND_DIRECTION_SAMPLE_DURATION = WIND_SPEED_SAMPLE_INTERVAL

    # Scale factors
    RAIN_SCALE_FACTOR = 0.2794 # 0.2794mm per contact, 2.54 mm per inch
    WIND_SPEED_SCALE_FACTOR = 2.4 / WIND_SPEED_SAMPLE_INTERVAL.total_seconds() / 3.6 # 1 Hz = 2.4km/hr, 1 m/s = 3.6 km/hr

    def __init__(self, listener=None):
        # Initialize readings
        self._readings = {
            "humidity": SensorReading(unit="%"),
            "pressure": SensorReading(unit="hPa"),
            "pressure-trend": SensorReading("hPa/hr"),
            "temperature": SensorReading(unit="°C"),
            "temperature-trend": SensorReading("°C/hr"),
            "humidity": SensorReading(unit="%"),
            "dew_point": SensorReading(unit="°C"),
            "wind_deg": SensorReading(unit="degrees"),
            "wind_gust": SensorReading(unit="m/s"),
            "wind_speed": SensorReading(unit="m/s"),
            "rain_1h": SensorReading(unit="mm"),
            "rain_6h": SensorReading(unit="mm"),
            "rain_24h": SensorReading(unit="mm")
        }

        # Run
        self.listener = listener
        BME280Sampler(self.BME280_I2C_BUS, self.AIR_SAMPLE_INTERVAL, self.AIR_SAMPLE_DURATION, self._bme280_sample).start()
        WindDirectionSampler(self.WIND_DIRECTION_RESISTOR, self.WIND_DIRECTION_ADC_CHANNEL, self.WIND_DIRECTION_SAMPLE_INTERVAL, self.WIND_DIRECTION_SAMPLE_DURATION, self._wind_direction_sample).start()
        ButtonSampler(self.WIND_SPEED_SENSOR_GPIO, self.WIND_SPEED_SAMPLE_INTERVAL, self.WIND_SPEED_SAMPLE_DURATION, self._wind_speed_sample).start()
        ButtonSampler(self.RAIN_SENSOR_GPIO, self.RAIN_SAMPLE_INTERVAL, self.RAIN_SAMPLE_DURATION, self._rain_sample).start()

    def __getattr__(self, name):
        if name not in self._readings:
            raise Exception(f"""{name} is not a valid attribute""")
        return self._readings.get(name)

    def _bme280_sample(self, data):
        self._readings.get("humidity").update(round(data[-1].humidity), data[-1].timestamp)
        self._readings.get("pressure").update(round(data[-1].pressure), data[-1].timestamp)
        self._readings.get("pressure-trend").update(round(self._trend([x.pressure for x in data]) * 3600 / self.AIR_SAMPLE_INTERVAL.total_seconds(), 1), data[-1].timestamp)
        self._readings.get("temperature").update(round(data[-1].temperature * 2) / 2, data[-1].timestamp)
        self._readings.get("temperature-trend").update(round(self._trend([x.temperature for x in data]) * 3600 / self.AIR_SAMPLE_INTERVAL.total_seconds(), 1), data[-1].timestamp)
        self._readings.get("dew_point").update(round(self.dew_point(data[-1].temperature, data[-1].humidity) * 2) / 2, data[-1].timestamp)
        self._publish({
            "humidity",
            "pressure",
            "pressure-trend",
            "temperature",
            "temperature-trend",
            "dew_point"
        })

    def _publish(self, keys: set):
        readings = {key: self._readings[key] for key in self._readings.keys() & keys}
        if self.listener is not None:
            threading.Thread(
                target=self.listener,
                args=(readings,)
            ).start()

    def _rain_sample(self, data):
        self._readings.get("rain_1h").update(self.RAIN_SCALE_FACTOR * sum(data[-int(3600 / self.RAIN_SAMPLE_INTERVAL.total_seconds()):]))
        self._readings.get("rain_6h").update(self.RAIN_SCALE_FACTOR * sum(data[-int(6 * 3600 / self.RAIN_SAMPLE_INTERVAL.total_seconds()):]))
        self._readings.get("rain_24h").update(self.RAIN_SCALE_FACTOR * sum(data))
        self._publish({"rain_1h", "rain_6h", "rain_24"})

    @staticmethod
    def _trend(data):
        if len(data) <= 1:
            return 0
        return numpy.polyfit(range(0, len(data)), data, 1)[0]

    def _wind_direction_sample(self, data):
        self._readings.get("wind_deg").update(round(self.average_angles(data)))
        self._publish({"wind_deg"})

    def _wind_speed_sample(self, data):
        self._readings.get("wind_gust").update(round(max(data) * self.WIND_SPEED_SCALE_FACTOR, 1))
        self._readings.get("wind_speed").update(round(numpy.mean(data) * self.WIND_SPEED_SCALE_FACTOR, 1))
        self._publish({"wind_gust", "wind_speed"})

    @property
    def readings(self):
        return self._readings

    @staticmethod
    def average_angles(angles):
        sin_sum = 0.0
        cos_sum = 0.0

        for angle in angles:
            r = math.radians(angle)
            sin_sum += math.sin(r)
            cos_sum += math.cos(r)

        flen = float(len(angles))
        s = sin_sum / flen
        c = cos_sum / flen
        try:
            arc = math.degrees(math.atan(s / c))
        except ZeroDivisionError:
            arc = 90
        average = 0.0

        if s > 0 and c >= 0:
            average = arc
        elif c <= 0:
            average = arc + 180
        elif s < 0 and c > 0:
            average = arc + 360

        return 0.0 if average == 360 else average

    @staticmethod
    def dew_point(temperature, humidity):
        b = 18.678
        c = 257.14
        gamma = math.log(humidity / 100) + b * temperature / (c + temperature)
        return c * gamma / (b - gamma)
