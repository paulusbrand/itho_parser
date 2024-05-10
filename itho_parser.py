import os
import sqlite3
import logging
import re
import yaml
from subprocess import Popen, PIPE
from tempfile import TemporaryDirectory
from shutil import which, copy

from homeassistant.components.sensor.const import (
    SensorDeviceClass,
    SensorStateClass,
    DEVICE_CLASS_UNITS,
    DEVICE_CLASS_STATE_CLASSES,
)


DEVICE_ID = "itho_432432"
ROOT_TOPIC = "itho_wtw"
ITHO_STATUS_TOPIC = f"{ROOT_TOPIC}/ithostatus"
AVAILABILITY_TOPIC = f"{ROOT_TOPIC}/lwt"
PAYLOAD_AVAILABLE = "online"
PAYLOAD_NOT_AVAILABLE = "offline"

# Directory where parameter files are located
PARAMETER_DIR = "parameters"

# Use a subset of avilable device classes
DEVICE_CLASSES = [
    SensorDeviceClass.APPARENT_POWER,
    SensorDeviceClass.CO2,
    SensorDeviceClass.CURRENT,
    SensorDeviceClass.DURATION,
    SensorDeviceClass.ENERGY,
    SensorDeviceClass.HUMIDITY,
    SensorDeviceClass.POWER,
    SensorDeviceClass.PRESSURE,
    SensorDeviceClass.TEMPERATURE,
    SensorDeviceClass.VOLUME_FLOW_RATE,
]


class HAMQTTSensorError(Exception):
    pass


class HAMQTTSensor:
    name: str
    unique_id: str

    availability_topic: str
    payload_available: str
    payload_not_available: str

    state_topic: str
    value_template: str

    device_class: SensorDeviceClass | None = None
    state_class: SensorStateClass | None = None
    unit_of_measurement: str | None = None
    fixed_unit_of_measurement: str | None = None

    def __init__(
        self,
        name: str,
        unique_id: str,
        availability_topic: str,
        payload_available: str,
        payload_not_available: str,
        state_topic: str,
        value: str,
        unit_of_measurement: str | None = None,
    ):
        self.name = name
        self.unique_id = unique_id

        self.unit_of_measurement = unit_of_measurement

        if self.unit_of_measurement:
            self.__fix_unit()
            self.__find_device_class()

        if self.device_class:
            state_classes = DEVICE_CLASS_STATE_CLASSES[self.device_class]
            if state_classes:
                self.state_class = list(state_classes)[0]

        self.availability_topic = availability_topic
        self.payload_available = payload_available
        self.payload_not_available = payload_not_available

        self.state_topic = state_topic
        self.value_template = (
            f"{{{{ value_json[\"{value} ({self.unit_of_measurement})\"] }}}}"
        )

    def __fix_unit(self) -> None:
        """Fixes common mistakes in units"""
        if self.unit_of_measurement == "M3/h":
            self.unit_of_measurement = "m3/h"
            self.fixed_unit_of_measurement = "m³/h"
        elif self.unit_of_measurement == "m3/h":
            self.fixed_unit_of_measurement = "m³/h"
        elif self.unit_of_measurement == "m³/h":
            self.unit_of_measurement = "m3/h"
            self.fixed_unit_of_measurement = "m³/h"
        elif self.unit_of_measurement == "uur":
            self.unit_of_measurement = "hour"
            self.fixed_unit_of_measurement = "h"
        elif self.unit_of_measurement == "-":
            self.fixed_unit_of_measurement = None
        else:
            self.fixed_unit_of_measurement = self.unit_of_measurement

    def __find_device_class(self) -> None:
        """Finds device class based on unit"""
        device_classes = [
            device_class
            for device_class in DEVICE_CLASSES
            if self.fixed_unit_of_measurement in DEVICE_CLASS_UNITS[device_class]
        ]

        if not len(device_classes):
            self.device_class = None
        elif len(device_classes) == 1:
            self.device_class = device_classes[0]
        else:
            raise HAMQTTSensorError("Multiple device classes found!")

    def to_dict(self) -> dict:
        sensor = {}
        sensor["name"] = self.name
        sensor["unique_id"] = self.unique_id
        sensor["state_topic"] = self.state_topic
        sensor["value_template"] = self.value_template
        if self.fixed_unit_of_measurement:
            sensor["unit_of_measurement"] = self.fixed_unit_of_measurement

        if self.device_class:
            sensor["device_class"] = str(self.device_class)

        if self.state_class:
            sensor["state_class"] = str(self.state_class)

        sensor["availability"] = [{"topic": self.availability_topic}]
        sensor["payload_available"] = self.payload_available
        sensor["payload_not_available"] = self.payload_not_available

        return sensor


class IthoParserError(Exception):
    pass


class IthoParameter:
    def __init__(
        self,
        Index: int,
        Volgorde: int,
        Naam: str,
        Naam_fabriek: str,
        Min: float,
        Max: float,
        Default: float,
        Tekst_NL: str,
        Omschrijving_NL: str,
        Eenheid_NL: str,
        Tekst_GB: str,
        Omschrijving_GB: str,
        Eenheid_GB: str,
        Tekst_D: str,
        Omschrijving_D: str,
        Eenheid_D: str,
        Subtabel: str,
        Paswoordnivo: int,
    ):
        self.Index = Index
        self.Volgorde = Volgorde
        self.Naam = Naam
        self.Naam_fabriek = Naam_fabriek
        self.Min = Min
        self.Max = Max
        self.Default = Default
        self.Tekst_NL = Tekst_NL
        self.Omschrijving_NL = Omschrijving_NL
        self.Eenheid_NL = Eenheid_NL
        self.Tekst_GB = Tekst_GB
        self.Omschrijving_GB = Omschrijving_GB
        self.Eenheid_GB = Eenheid_GB
        self.Tekst_D = Tekst_D
        self.Omschrijving_D = Omschrijving_D
        self.Eenheid_D = Eenheid_D
        self.Subtabel = Subtabel
        self.Paswoordnivo = Paswoordnivo

    def to_yaml(self) -> str:
        pass


class IthoDatalabel:
    def __init__(
        self,
        Index: int,
        Naam: str,
        Tekst_NL: str,
        Tooltip_NL: str,
        Eenheid_NL: str,
        Tekst_GB: str,
        Tooltip_GB: str,
        Eenheid_GB: str,
        Tekst_D: str,
        Tooltip_D: str,
        Eenheid_D: str,
        SubTabel: str,
        Visible: int,
    ):
        self.Index = Index
        self.Naam = Naam
        self.Tekst_NL = Tekst_NL
        self.Tooltip_NL = Tooltip_NL
        self.Eenheid_NL = Eenheid_NL
        self.Tekst_GB = Tekst_GB
        self.Tooltip_GB = Tooltip_GB
        self.Eenheid_GB = Eenheid_GB
        self.Tekst_D = Tekst_D
        self.Tooltip_D = Tooltip_D
        self.Eenheid_D = Eenheid_D
        self.SubTabel = SubTabel
        self.Visible = Visible

    def __str__(self):
        return f"{self.Index} | {self.Naam} | {self.Tekst_GB} | {self.Tooltip_GB} | {self.Eenheid_GB}"


class IthoParser:
    connection = None

    def __init__(self, parameter_file: str):
        """This method creates a new IthoParser instance"""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.versions: list[str] = []
        self.parameters: dict = {}
        self.datalabels: dict = {}
        self.tables: list[str] = []

        if not which("mdb-schema"):
            raise IthoParserError(
                "`mdb-schema` executable not found. Make sure mdbtools is installed and in PATH"
            )

        self.temp_dir = TemporaryDirectory()

        file = os.path.split(parameter_file)[1]
        if file.endswith(".par"):
            file = file.replace(".par", ".mdb")
        tmp_file = os.path.join(self.temp_dir.name, file)
        try:
            copy(parameter_file, tmp_file)
        except FileNotFoundError as err:
            raise IthoParserError(
                f"Parameter file not found: {parameter_file}"
            ) from err

        self.parameter_file = tmp_file
        self.logger.debug(f"Created temporary file: {tmp_file}")

        # Create destination sqlite database
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()

    def __del__(self):
        if self.connection:
            self.connection.commit()
            self.connection.close()

    def parse(self) -> None:
        """Converts the parameter file to an sqlite database

        Steps:
        1. Create a directory to hold the temporary table exports
        2. Extract the database schema and save to file
        3. Extract all the table names and skip the ones starting with "~"
        4. Apply the exported database schema to the new database
        5. Import each table

        """
        file_name = os.path.split(self.parameter_file)[1].split(".")[0]
        table_dir = os.path.join(self.temp_dir.name, file_name)
        os.makedirs(table_dir)
        self.logger.debug(f"Created temporary table directory: {table_dir}")

        # Export database schema
        schema_file = os.path.join(table_dir, "schema.sqlite")
        with open(schema_file, "wb") as schema:
            schema_command = ["mdb-schema", self.parameter_file, "sqlite"]
            proces = Popen(schema_command, stdout=schema, stderr=PIPE)
            _, std_error = proces.communicate()
            if std_error:
                raise IthoParserError(f"Failed to export schema: {std_error}")
        self.logger.debug(f"Exported database schema to {schema_file}")

        # Get table names
        tables_command = ["mdb-tables", "-1", self.parameter_file]
        proces = Popen(tables_command, stdout=PIPE, stderr=PIPE)
        std_out, std_error = proces.communicate()
        if std_error:
            raise IthoParserError(f"Failed to get database tables: {std_error}")
        tables = std_out.decode("ascii").strip().split("\n")

        # Filter temporary tables starting with "~"
        self.tables = [table for table in tables if not table.startswith("~")]
        for table in self.tables:
            self.logger.debug(f"Found database table: {table}")

        # Apply database schema
        with open(schema_file, "r") as schema:
            self.cursor.executescript(schema.read())

        self.connection.commit()

        # Export tables to sql and insert into destination database
        for table_name in self.tables:
            table_file = os.path.join(table_dir, f"{table_name}.sql")
            export_command = [
                "mdb-export",
                "-D",
                "'%Y-%m-%d %H:%M:%S'",
                "-q",
                "'",
                "-H",
                "-I",
                "sqlite",
                self.parameter_file,
                table_name,
            ]
            with open(table_file, "wb") as table:
                proces = Popen(export_command, stdout=table, stderr=PIPE)
                _, std_error = proces.communicate()
                if std_error:
                    raise IthoParserError(
                        f"Failed to convert table: {table_name} with error: {std_error}"
                    )

            # Insert into destination database
            with open(table_file, "r") as table:
                sql = table.read()
                self.cursor.executescript(sql)
            self.logger.debug(f"Converted table: {table_name}")

        self.connection.commit()

    def find_versions(self) -> None:
        """Find versions based on table names"""
        version_match = ".+V[0-9]{1,2}$"
        max_version = 0
        version_tables = [
            table for table in self.tables if re.match(version_match, table)
        ]
        for table in version_tables:
            version = int(table.split("_V")[-1])
            if version > max_version:
                max_version = version

        self.versions = [version for version in range(1, max_version + 1, 1)]
        self.logger.debug(f"Found versions: {self.versions}")

    def find_parameters(self) -> None:
        """Find parameters for each version"""

        current_table = ""
        for version in self.versions:
            self.logger.debug(f"Finding parameters for version {version}")

            if f"Parameterlijst_V{version}" in self.tables:
                current_table = f"Parameterlijst_V{version}"
            elif f"parameterlijst_V{version}" in self.tables:
                current_table = f"Parameterlijst_V{version}"

            self.logger.debug(f"Using table: {current_table}")

            query = f'SELECT * FROM {current_table} ORDER BY "Index" ASC'
            result = self.cursor.execute(query)

            new_parameters = []
            for parameter in result:
                new_parameter = IthoParameter(**parameter)
                new_parameters.append(new_parameter)
                # self.logger.debug(parameter.keys())
                self.logger.debug(
                    f"Found parameter id: {parameter['Index']} name: {parameter['Tekst_NL']}"
                )
            self.parameters[version] = new_parameters

    def find_datalabels(self) -> None:
        """Find datalabels for each version"""

        current_table = ""
        for version in self.versions:
            self.logger.debug(f"Finding datalabels for version {version}")

            if f"Datalabel_V{version}" in self.tables:
                current_table = f"Datalabel_V{version}"

            self.logger.debug(f"Using table: {current_table}")

            query = f'SELECT * FROM {current_table} ORDER BY "Index" ASC'
            result = self.cursor.execute(query)

            new_datalabels = []
            for datalabel in result:
                new_datalabel = IthoDatalabel(**datalabel)
                new_datalabels.append(new_datalabel)

                self.logger.debug(f"Found datalabel {str(new_datalabel)}")
            self.datalabels[version] = new_datalabels

    def get_versions(self) -> list[str]:
        return self.versions

    def get_ha_sensors(self, version: str) -> list[HAMQTTSensor]:
        if not version in self.versions:
            raise IthoParserError(f"Firmware version: {version} not found")

        sensors = []
        for datalabel in self.datalabels[version]:
            sensor = HAMQTTSensor(
                name=datalabel.Tekst_GB,
                unique_id=f"{DEVICE_ID}_{datalabel.Tekst_GB}",
                availability_topic=AVAILABILITY_TOPIC,
                payload_available=PAYLOAD_AVAILABLE,
                payload_not_available=PAYLOAD_NOT_AVAILABLE,
                state_topic=ITHO_STATUS_TOPIC,
                value=datalabel.Tooltip_GB,
                unit_of_measurement=datalabel.Eenheid_GB,
            )
            sensors.append(sensor)

        return sensors


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, handlers=[logging.StreamHandler()])

    logger = logging.getLogger(__name__)

    p = IthoParser(os.path.join(PARAMETER_DIR, "$_parameters_HRU250-300.par"))
    p.parse()
    p.find_versions()
    p.find_parameters()
    p.find_datalabels()
    versions = p.get_versions()
    sensors = p.get_ha_sensors(versions[-1])

    print(
        yaml.dump(
            [sensor.to_dict() for sensor in sensors],
            allow_unicode=True,
            sort_keys=False,
            width=1000,
        )
    )
