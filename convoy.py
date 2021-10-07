import json
import sqlite3

import pandas as pd


file_name, file_extension = input('Input file name\n').split('.')
file_extension = '.' + file_extension


class InformationInterface:
    def __init__(self, num: int, name: str):
        self.num = num
        self.name = name

    def resulting(self):
        pass


class LineInfo(InformationInterface):
    def resulting(self):
        if int(self.num) == 1:
            print(f'{self.num} line was added to {self.name}.csv')
        elif int(self.num) > 1:
            print(f'{self.num} lines were added to {self.name}.csv')


class CellInfo(InformationInterface):
    def resulting(self):
        if int(self.num) == 1:
            print(f'{self.num} cell was corrected in {self.name}[CHECKED].csv')
        elif int(self.num) > 1:
            print(f'{self.num} cells were corrected in {self.name}[CHECKED].csv')


class RecordInfo(InformationInterface):
    def resulting(self):
        if int(self.num) == 1:
            print(f'{self.num} record was inserted into {self.name}.s3db')
        elif int(self.num) > 1:
            print(f'{self.num} records were inserted into {self.name}.s3db')


class VehicleJsonInfo(InformationInterface):
    def resulting(self):
        if int(self.num) == 1:
            print(f'{self.num} vehicle was saved into {self.name}.json')
        elif int(self.num) > 1:
            print(f'{self.num} vehicles were saved into {self.name}.json')


class VehicleXmlInfo(InformationInterface):
    def resulting(self):
        if int(self.num) == 1:
            print(f'{self.num} vehicle was saved into {self.name}.xml')
        else:
            print(f'{self.num} vehicles were saved into {self.name}.xml')


class ScoreCounter:
    def __init__(self, tuple_result):
        self.capacity = tuple_result[0]
        self.fuel = tuple_result[1]
        self.load = tuple_result[2]
        self.score = 0
        self.burn_fuel = 0

    def capacity_score(self):
        if self.load >= 20:
            self.score += 2

    def consumed_score(self):
        distance = 450
        self.burn_fuel = distance * self.fuel / 100
        if self.burn_fuel < 230:
            self.score += 2
        else:
            self.score += 1

    def pitstop_score(self):
        pit_stop = self.burn_fuel / self.capacity
        if pit_stop < 1:
            self.score += 2
        elif pit_stop < 2:
            self.score += 1

    def multi_score(self):
        self.capacity_score()
        self.consumed_score()
        self.pitstop_score()
        return self.score


class FileHandler:
    def __init__(self, name: str, trigger=False, checked=False):
        self.name = name
        self.trigger = trigger
        self.checked = checked

    def file_reader(self):
        if self.trigger:
            return pd.read_csv(self.name + '[CHECKED].csv')
        return pd.read_csv(self.name + '.csv')

    def connect(self):
        if self.checked:
            return sqlite3.connect(self.name[:-9] + '.s3db')
        return sqlite3.connect(self.name + '.s3db')

    def file_converter(self):
        pass


class ExcelToCsv(FileHandler):
    def file_reader(self):
        return pd.read_excel(self.name + '.xlsx',
                             sheet_name='Vehicles',
                             dtype=str)

    def file_converter(self):
        self.file_reader().to_csv(f'{self.name}.csv',
                                  index=None,
                                  header=True)

    def line_number(self):
        self.file_converter()
        return self.file_reader()


class CsvCleaning(FileHandler):
    def data_cleaning(self):
        df = self.file_reader()
        vec = [str(x) for x in df['vehicle_id']]
        df['vehicle_id'] = vec
        incorrect_value = df.to_dict(orient='list')
        for k, v in incorrect_value.items():
            cleaning_data = [''.join([x for x in value if x.isdigit()])
                             for value in incorrect_value[k]]
            df.update({k: cleaning_data})
        df.to_csv(f'{self.name}[CHECKED].csv', index=None, header=True)

    def mistake_counter(self):
        self.data_cleaning()
        df = self.file_reader()
        vec = [str(x) for x in df['vehicle_id']]
        df['vehicle_id'] = vec
        new_lst = [[x for x in value if not x.isdigit()]
                   for value in df.iloc[range(df.shape[0])].values]
        return len([value for elements in new_lst for value in elements])


class CsvToSql(FileHandler):
    def file_converter(self):
        df = self.file_reader()
        tuple_score = tuple(ScoreCounter(elem).multi_score()
                            for elem in df.iloc[:, 1:].values)
        df['score'] = tuple_score
        df = df.set_index('vehicle_id')
        headers = df.columns
        headers = {}.fromkeys(headers, "INTEGER NOT NULL")
        headers['vehicle_id'] = 'INTEGER PRIMARY KEY'
        df.to_sql('convoy',
                  self.connect(),
                  if_exists='replace',
                  dtype=headers)

    def database_info(self):
        selection = """SELECT * FROM convoy;"""
        with self.connect():
            return len(self.connect().execute(selection).fetchall())


class SqlToJsonXml(FileHandler):
    def file_converter(self):
        query_for_json = """SELECT vehicle_id, 
        engine_capacity, 
        fuel_consumption, 
        maximum_load 
        FROM convoy WHERE score > 3;"""
        query_for_xml = """SELECT vehicle_id, 
        engine_capacity, 
        fuel_consumption, 
        maximum_load 
        FROM convoy WHERE score <= 3;"""
        val_for_json = pd.read_sql_query(query_for_json,
                                         self.connect()).to_dict(orient='records')
        val_for_xml = pd.read_sql_query(query_for_xml,
                                        self.connect()).to_dict(orient='records')
        json_data_file = {"convoy": val_for_json}
        with open(f'{self.name}.json', 'w', encoding='utf-8') as file_:
            json.dump(json_data_file, file_, indent=2)
        tag_s = '<vehicle>'
        tag_e = '</vehicle>'
        xml_str = ''.join([tag_s + ''.join([f'<{k}>{v}</{k}>'
                                            for k, v in elem.items()]) + tag_e
                           for elem in val_for_xml])
        final_xml = '<convoy>' + xml_str + '</convoy>'
        with open(f'{self.name}.xml', 'w', encoding='utf-8') as file:
            file.write(final_xml)

    def json_info(self):
        query_json = """SELECT * FROM convoy WHERE score > 3;"""
        with self.connect():
            return len(self.connect().execute(query_json).fetchall())

    def xml_info(self):
        query_xml = """SELECT * FROM convoy WHERE score <= 3;"""
        with self.connect():
            return len(self.connect().execute(query_xml).fetchall())


def excel_handler(name):
    number = ExcelToCsv(name).line_number().shape[0]
    LineInfo(number, name).resulting()
    mistake = CsvCleaning(name).mistake_counter()
    CellInfo(mistake, name).resulting()
    CsvToSql(name, trigger=True).file_converter()
    RecordInfo(number, name).resulting()
    sql = SqlToJsonXml(name)
    sql.file_converter()
    VehicleJsonInfo(sql.json_info(), name).resulting()
    VehicleXmlInfo(sql.xml_info(), name).resulting()


def csv_handler_checked(name):
    csv_to_db = CsvToSql(name, checked=True)
    csv_to_db.file_converter()
    number = csv_to_db.database_info()
    name = name[:-9]
    RecordInfo(number, name).resulting()
    sql = SqlToJsonXml(name)
    sql.file_converter()
    VehicleJsonInfo(sql.json_info(), name).resulting()
    VehicleXmlInfo(sql.xml_info(), name).resulting()


def csv_handler_not_checked(name):
    mistake = CsvCleaning(name).mistake_counter()
    CellInfo(mistake, name).resulting()
    sql = CsvToSql(name, trigger=True)
    sql.file_converter()
    records = sql.database_info()
    RecordInfo(records, name).resulting()
    sql = SqlToJsonXml(name)
    sql.file_converter()
    VehicleJsonInfo(sql.json_info(), name).resulting()
    VehicleXmlInfo(sql.xml_info(), name).resulting()


def sql_handler(name):
    sql = SqlToJsonXml(name)
    sql.file_converter()
    VehicleJsonInfo(sql.json_info(), name).resulting()
    VehicleXmlInfo(sql.xml_info(), name).resulting()


if file_extension == '.xlsx':
    excel_handler(file_name)
elif file_extension == '.csv' and '[CHECKED]' in file_name:
    csv_handler_checked(file_name)
elif file_extension == '.csv':
    csv_handler_not_checked(file_name)
elif file_extension == '.s3db':
    sql_handler(file_name)
