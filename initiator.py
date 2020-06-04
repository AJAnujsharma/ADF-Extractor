import json
from datetime import date, datetime
import pathlib, os, sys
import xlrd
import xlsxwriter
from django.db.models import QuerySet
from enum import Enum
from typing import List, Union, Dict, Tuple
from django.db import models
from decimal import Decimal
import copy

class DBUtility:
    """
    Class to facilitate DB conversions and metadata operations. this utility class is NOT meant to do
    live DB updates.
    """

    _ORM_MAP = {
        'CharField': 'string',
        'BigIntegerField': 'number',
        'BinaryField': 'string',
        'BooleanField': 'boolean',
        'CommaSeparatedIntegerField': 'list',  # of ints
        'DateField': 'date',
        'DateTimeField': 'datetime',
        'DecimalField': 'number',
        'DurationField': 'number',
        'EmailField': 'string',
        'FileField': 'string',
        'FilePathField': 'string',
        'FloatField': 'number',
        'ImageField': 'string',
        'IntegerField': 'number',
        'IPAddressField': 'number',
        'NullBooleanField': 'boolean',
        'PositiveIntegerField': 'number',
        'PositiveSmallIntegerField': 'number',
        'SlugField': 'string',
        'SmallIntegerField': 'string',
        'TextField': 'string',
        'TimeField': 'datetime',
        'URLField': 'string',
        'UUIDField': 'string',
    }

    @staticmethod
    def get_table_cls(table_name: str):
        """
        Gets the ORM class which corresponds to a table name in the eba pipelines DB. If none is found, it will
        raise an Exception

        :param table_name: the table name as a string which will be converted to an ORM class if found
        :return: the class or raise an exception
        """
        cls = DBUtility._table_mappings.get(table_name.lower())  # type: models.Model
        if cls is None:
            raise ValueError("No corresponding pipeline table found for name: {}".format(table_name))
        return cls

    @staticmethod
    def get_table_columns(table_name: Union[str, models.Model], ignore_list: List[str] = None,
                          to_fetch: List[str] = None) -> Dict[str, models.Field]:
        """
        Gets a dictionary of the model fields and their corresponding types in the DB for a specific table. Returns
        something like:
        {
            'id': <django.db.models.fields.AutoField: id>,
            'project_definition': <django.db.models.fields.CharField: project_definition>,
            'wbs_element': <django.db.models.fields.CharField: wbs_element>,
            ...
        }

        :param table_name: a string of the actual ORM class for a django EBA or pipelines ORM model

        :param ignore_list: specifies a list of columns to ignore when getting all the field attributes of an
            ORM class
\
        :param to_fetch: an explicit list of columns to fetch and convert. If this is passed in, it will not
            use the default ignore list which excludes the ID and foreign key automatically.
        :return: a dictionary or raises an error if the table class could not be found
        """
        if isinstance(table_name, str):
            table_name = DBUtility.get_table_cls(table_name)
        temp = model_fields = table_name._meta._forward_fields_map
        if to_fetch:
            temp = {}
            for col in to_fetch:
                if col in model_fields:
                    temp[col] = model_fields[col]
        elif ignore_list:
            temp = {}
            for key in model_fields.keys():
                if key not in ignore_list:
                    temp[key] = model_fields[key]
        model_fields = temp
        return model_fields

    @staticmethod
    def convert_column_type(column_type: models.fields_all) -> str:
        """
        Converts the column type from the original django db model field to a string representation which can be
        passed onto external services. Example, a db CharField will convert to a 'string' field and a DB decimal field
        will convert to a 'number' field for typescript/js consumption

        :param column_type: an instance of models.fields
        :return:
        """
        return DBUtility._ORM_MAP.get(column_type.get_internal_type())

    @staticmethod
    def convert_to_human_name(name: str, remove: str = None) -> str:
        """
        Converts a postgres DB column or table name to a human readable name by removing underscores and
        capitalizing letters

        :param name: the name to convert
        :param remove: the string to remove from the name passed in - it will return the human readable name
            after that bit of the name has been removed. I.e., zprod_wbs_person --> WBS Person after 'zprojd' is given

        :return: a string representation of that name
        """
        if name is None:
            raise ValueError("invalid name passed in for conversion: {}".format(name))
        name = name.strip()
        if name == '':
            raise ValueError("invalid name passed in for conversion: {}".format(name))
        if remove:
            name = ''.join(name.split(remove))
        x = name.split('_')
        for idx, word in enumerate(x):
            if len(word) == 1:
                word = word.upper()
            else:
                first = word[0].upper()
                word = first + word[1:]
            x[idx] = word
        return ' '.join(x)

    @staticmethod
    def bulk_update_orm(orm_cls: models.Model, id_list: List[int], column_name: str, new_value,
                        apply_func_instructions=None) -> Tuple[bool, List]:
        """
        Takes in an ORM class and applies a bulk update to a specific column in the ORM class with a new value
        provided. It uses a list of ID's passed in to know what objects to update. This method will attempt to
        rollback any failed commits

        :param orm_cls: the class which is an instance of django's models.Model class
        :param id_list: a list of primary keys which are IDs
        :param column_name: the column to update
        :param new_value: the new value
        :param apply_func_instructions: a custom function call to run on the required columns DURING the update
            for each record. example: If a hash function was passed in for column1 and 2, it would concatenate
            the columns and then apply this hash function on those columns

        :return: True or False if the update was successful along with any errors as a tuple
        """
        old_values = {}
        errors = []
        if apply_func_instructions:
            print('Got apply func {} for #{} of items to update with '
                                   'this function'.format(apply_func_instructions, len(id_list)))

        for pk in id_list:
            obj = orm_cls.objects.get(pk=pk)
            old_values[pk] = obj  # save the value for later if rollback is necessary
            try:
                setattr(obj, column_name, new_value)
                if apply_func_instructions:
                    func = apply_func_instructions.get('function')
                    cols = apply_func_instructions.get('columns')
                    instruction = apply_func_instructions.get('instruction', 'concatenate')
                    col_to_set = apply_func_instructions.get('col_to_set')
                    if func and cols and col_to_set:
                        vals_to_use = []
                        for col in cols:
                            vals_to_use.append(str(getattr(obj, col, '')))
                        if instruction == 'concatenate':
                            res = ''.join(vals_to_use)
                        else:
                            raise RuntimeError('Invalid instruction passed in - for now the only instruction available '
                                               'is concatenate. Passed in: {}'.format(instruction))
                        res = func(res)
                        setattr(obj, col_to_set, res)
                obj.save()
            except Exception as e:
                errors.append(str(e))
                print("Error while saving orm {} with ID {} when setting new value {} "
                                            "for column {}. Error: {}".format(orm_cls, pk, new_value, column_name, e))
        if errors:
            print("Bulk update errors were detected. Attempting to rollback now!")
            for pk, orm in old_values.items():
                try:
                    orm.save()
                except Exception as e:
                    print("Error while attempting to roll back for id {}. "
                                                "Error: {}".format(pk, e))
        else:
            print('No rollback necessary while doing bulk update for orm {} and Ids {} for'
                                   ' column {} and value {}'.format(orm_cls, id_list, column_name, new_value))
        return (True, []) if len(errors) == 0 else (False, errors)

    @staticmethod
    def serialize(obj):
        """
        Serializes any object so it can be returned to the frontend. At the time of writing this it will do strict
        conversions for datetime objects, Decimals, and BigInts. Bytes objects will return a None type when serialized

        :param obj: the object to serialize
        :return: the serialized type which can be converted to JSON. Returns None if no serializer value can be found
        """
        if isinstance(obj, datetime) or isinstance(obj, date):
            return obj.strftime('%m/%d/%Y')
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, str) or isinstance(obj, int) or isinstance(obj, dict) \
                or isinstance(obj, bool) or isinstance(obj, float):
            return obj
        return None

    @staticmethod
    def extract_table_name(orm_cls, chop_off: str = None, multiple=True) -> Union[str, None]:
        """
        Returns the string table name of the Django ORM model. if given a "chop off" string, it will search for the
        chop_off string and chop it off of the table name if it finds it and returns the table name.

        :param orm_cls: the Django orm class to extract the table name from
        :param chop_off: what to chop off if relevant
        :param multiple: If true, this will remove all instances of the chop_off string. If False, it will remove
            only the first instance it finds.

        :return: the table name as a string or None if no table name found
        """
        # EbaPipelineOperationalBudgetAo._meta.db_table
        if not orm_cls:
            return None
        table_name = orm_cls._meta.db_table  # type: str
        if not chop_off:
            return table_name
        if table_name.find(chop_off) == -1:
            return table_name
        kwargs = {'sep': chop_off}
        kwargs.update({'maxsplit': 1}) if not multiple else kwargs.update({})
        s_list = table_name.split(**kwargs)
        return ''.join(s_list)


class CellBorder(Enum):
    TOP = 'top'
    BOTTOM = 'bottom'
    LEFT = 'left'
    RIGHT = 'right'

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other.lower()
        elif isinstance(other, CellBorder):
            return self == other
        return False


class FontStyle(Enum):
    BOLD = 'bold'
    ITALICS = 'italics'
    UNDERLINE = 'underline'
    STRIKETHROUGH = 'strikethrough'

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other.lower()
        elif isinstance(other, FontStyle):
            return self == other
        return False


class Formatter:

    def __init__(self, cell_str_format: str = None, font_styles: List[FontStyle] = None,
                 cell_borders: List[CellBorder] = None, text_color: str = None, bg_color: str = None):
        """
        Initialize a custom formatter object that can create XLFS Styles on demand for the XLWT Writer

        :param cell_str_format: a string formatting representation compatible with XLWT. This can be numerical
            formatting that XLWT uses like "$%d.00" for instance
        :param font_styles: a list of whether or not the
        :param cell_borders: a list of CellBorder enums to determine how many borders to apply to a particular cell
        :param text_color: the text color of the cell
        :param bg_color: the background color of the cell
        """
        self.cell_str_format = cell_str_format
        self.font_styles = font_styles
        self.cell_borders = cell_borders
        self.text_color = text_color
        self.bg_color = bg_color

    # @property
    # def style(self):
    #     return self.xf_style

    def set_format(self, input_format:format) -> Union[str, None]:

        if self.cell_str_format is not None:
            input_format.set_num_format(self.cell_str_format)

        if self.text_color is not None:
            input_format.set_font_color(self.text_color)

        if self.bg_color is not None:
            input_format.set_bg_color(self.bg_color)
        if self.font_styles is not None:
            for fs in self.font_styles:
                if fs == "bold":
                    input_format.set_bold()
                if fs == "italics":
                    input_format.set_italic()
                if fs == "underline":
                    input_format.set_underline()
                if fs == "strikethrough":
                    input_format.set_font_strikeout()


        if self.cell_borders is not None:
            for border in self.cell_borders:
                if border == "top":
                    input_format.set_top(2)
                if border == "bottom":
                    input_format.set_bottom(2)
                if border == "left":
                    input_format.set_left(2)
                if border == "right":
                    input_format.set_right(2)


class ExcelUtils:
    """
    Excel file utilities to read a workbook and write to a workbook
    """
    def __init__(self):
        self.workbook = None
        self.current_sheet = None
        self.path_or_bytes_stream = None
        self._offset = 0

    def read_file(self, path: str = None, raw_file=None, sheet: str = None) -> int:
        """
        Reads an existing excel workbook from a give file path

        :param path: the absolute file path for an excel file
        :param sheet: the required sheet name the user needs. If this is empty, this will
        choose the first valid sheet it finds in the workbook
        :param raw_file: The excel file itself (usually as bytes) -- this will allow xlrd to read a
            file from memory instead of reading it from a path on the system. No need to save a file in this case...
        :return: the number of rows found in the current sheet
        """
        if path and raw_file:
            print("Both a path ( {} ) and a raw file was provided. I chose to read from the path "
                                 "instead".format(path))
        if path:
            if self.workbook is None:  # no need to waste time opening it again if it's already in memory
                self.workbook = xlrd.open_workbook(filename=path)
                self.path_or_bytes_stream = path
        elif raw_file:
            if self.workbook is None:
                self.workbook = xlrd.open_workbook(file_contents=raw_file.read())
                self.path_or_bytes_stream = raw_file
        else:
            raise RuntimeError("No path or raw file provided to read an excel file...what do you want me to do?")

        if sheet:
            self.current_sheet = self.workbook.sheet_by_name(sheet)
        else:
            self.current_sheet = self.workbook.sheet_by_index(0)
        return self.current_sheet.nrows

    def create_new(self, path: str, values, overwrite: bool = False, sheet_name: str = 'Sheet1',
                   separate_headers: list = None, instructions: dict = None):
        """
        Writes to a new excel file at a given path
        :param path: the path to the new excel file
        :param values: a list of lists -- the outer list is the rows and each sublist is the values for the
        respective column indices
        :type values: list[list]
        :param sheet_name: the sheet name to create in this new excel file, by default it is Excel's Sheet1
        :param overwrite: whether to overwrite an existing Excel file at a particular path
        :param separate_headers: a list of headers to put into the workbook. If this value is passed in, then the
            headers will be written before any of the values
        :param instructions: a set of formatting instructions for the row(s) to be written. These can be row
            by row instructions, or column by column instruction or cell by cell instruction
        :return: the offset - the next row index available for writing
        """
        if os.path.exists(path):
            if not overwrite:
                msg = "File at path {} already exists. Please explicitly say " \
                      "you with to override this file".format(path)
                print(msg)
                raise RuntimeError(msg)
        if not self.workbook:
            self.path = path
            self.workbook = xlsxwriter.Workbook(path)
        if not self.current_sheet:
            self.current_sheet = self.workbook.add_worksheet(sheet_name)
        if separate_headers:
            for idx, header in enumerate(separate_headers):
                formatter = self._convert_instructions_to_formats(instructions, 0, idx)
                self.write_cell(0, idx, header, formatter)

        offset = 1 if separate_headers else 0

        if isinstance(values, QuerySet):
            count = values.count()
            print("Detected query set with count {} for file {}".format(count, path))
        else:
            count = len(values)
        row_idx = 0
        for row_idx in range(count):
            row = values[row_idx]
            if isinstance(row, list) or isinstance(row, tuple):
                for col_idx in range(len(row)):
                    value = DBUtility.serialize(row[col_idx])
                    formatter = self._convert_instructions_to_formats(instructions, row_idx + offset, col_idx)
                    self.write_cell(row_idx + offset, col_idx, value, formatter)
            elif isinstance(row, dict):
                pass
        self.path_or_bytes_stream = path
        self._offset = row_idx + 1 + offset
        return row_idx + 1 + offset

    def create_new_sheet(self, values, separate_headers: list = None, sheet_name: str = 'Sheet2', overwrite=True,
                         instructions: dict = None):
        """
        Creates a new sheet to an existing workbook

        :param values: the rows to write
        :param separate_headers: if given, these will be headers written first before the values
        :param sheet_name: the new sheet to write to
        :param overwrite: True by default, it will overwrite the all the  values in a sheet if it exists
        :param instructions: a set of formatting instructions for the row(s) to be written. These can be row
            by row instructions, or column by column instruction or cell by cell instruction
        :return: the offset - the next available row to write to
        """

        # self.current_sheet = self.workbook.add_sheet(sheet_name, cell_overwrite_ok=overwrite)
        self.current_sheet = self.workbook.add_worksheet(sheet_name)

        if separate_headers:
            for idx, header in enumerate(separate_headers):
                formatter = self._convert_instructions_to_formats(instructions, 0, idx)
                self.write_cell(0, idx, header, formatter)

        offset = 1 if separate_headers else 0
        if isinstance(values, QuerySet):
            count = values.count()
            print("Detected query set with count {} for file {}".format(count, self.path_or_bytes_stream))
        else:
            count = len(values)
        row_idx = 0
        for row_idx in range(count):
            row = values[row_idx]
            if isinstance(row, list) or isinstance(row, tuple):
                for col_idx in range(len(row)):
                    value = DBUtility.serialize(row[col_idx])
                    formatter = self._convert_instructions_to_formats(instructions, row_idx+offset, col_idx)
                    self.write_cell(row_idx + offset, col_idx, value, formatter)
            elif isinstance(row, dict):
                pass
        self._offset = row_idx + 1 + offset
        return row_idx + 1 + offset

    def read_sheet(self, sheet_name='Sheet1', row_start: int = 0, row_end: int = None, col_start: int = 0,
                   col_end: int = None):
        """
        Returns all the rows in an excel file in a given sheet

        :param sheet_name: The sheet to read of the currently open workbook
        :param row_start: the row number to start reading - STARTS AT INDEX 0 which may be the header of a file
        :param row_end: when to stop - if no row end is given, it will by default read the entire sheet. KEEP IN MIND
            ZERO INDEXING IS USED. SO THIS IS THE INDEX POSITION OF THE LAST INDEX
        :param col_start: the starting column to read
        :param col_end: the column to end at

        :return: a list of lists which are all the rows in the excel
        """
        values = []
        if row_end is None:
            row_end = self.read_file(path=self.path_or_bytes_stream, sheet=sheet_name) - 1  # -1 because of 0 indexing
        for i in range(row_start, row_end + 1):
            row_val = self.row_values(row_idx=i, start_idx=col_start, end_idx=col_end)
            values.append(row_val)
        return values

    # new method added
    def write_cell(self, row_idx:int, col_idx: int, value:str, formatter:Formatter):
        cell_format = self.workbook.add_format()
        formatter.set_format(cell_format) # set format
        self.current_sheet.write(row_idx, col_idx, value, cell_format)


    def write_row(self, values: list, offset: int = None, instructions: dict = None) -> int:
        """
        Writes a list of values to an existing excel file - it writes to the row index offset specified

        :param values: a list of values to write to the excel file
        :param offset: what row to begin writing (XlsWriter) uses index 0 as the first row)
        :param instructions: a set of formatting instructions for the row(s) to be written. These can be row
            by row instructions, or column by column instruction or cell by cell instruction
        :return the next offset
        """
        if len(values) == 0:
            is_nested = None
        else:
            is_nested = isinstance(values[0], list) or isinstance(values[0], tuple)
        offset = self._offset if offset is None else offset
        new_offset = offset
        if is_nested == True:
            for row_idx, row in enumerate(values):
                for col_idx, item in enumerate(row):
                    value = DBUtility.serialize(item)
                    formatter = self._convert_instructions_to_formats(instructions, row_idx, col_idx)
                    self.write_cell(row_idx + offset, col_idx, value, formatter)
                new_offset += 1
        elif is_nested == False:
            for col_idx, item in enumerate(values):
                value = DBUtility.serialize(item)
                formatter = self._convert_instructions_to_formats(instructions, offset, col_idx)
                self.write_cell(offset, col_idx, value, formatter)
            new_offset += 1
        self._offset = new_offset
        return new_offset

    def get_rows(self, filters: list = None, grab_headers: bool = False, sanitize_dates=None):
        """
        Gets all the rows in an excel sheet and can even filter the rows based on specific values. These
        filters must be a list of equality checks (for now)

        :param grab_headers: whether to return the column headers as part of the rows
        :param filters: must be an ordered list of filters to apply
        :param sanitize_dates: if any indices are given in this list, it means that the value in that cell must be
            converted to a python datetime object
        :type sanitize_dates: list[int]

        :return: a list of rows with filters applied
        """
        rows = []
        if grab_headers and not filters:
            # it means we should return all the rows
            rows = self.current_sheet.get_rows()
        start = 0 if grab_headers else 1
        for row_num in range(start, self.current_sheet.nrows):
            row = self.current_sheet.row(row_num)  # type: list[xlrd.sheet.Cell]
            row_values = []
            disregard = False
            for cell_idx in range(len(row)):
                cell_val = row[cell_idx].value
                if filters:
                    for filter_idx in range(len(filters)):
                        if filter_idx == cell_idx:  # if the columns align, check the value
                            if cell_val != filters[filter_idx]:
                                disregard = True
                if not disregard:
                    if sanitize_dates and cell_idx in sanitize_dates:
                        if cell_val is not None and (isinstance(cell_val, int) or isinstance(cell_val, float)):
                            cell_val = xldate_as_datetime(cell_val, self.workbook.datemode)
                        elif cell_val is None or cell_val == '':
                            cell_val = None
                        elif isinstance(cell_val, str):
                            is_datetime, cell_val = self.is_datetime(cell_val)
                        else:
                            raise RuntimeError("Expected a Date field in column {}. Got {} value "
                                               "instead".format(cell_idx + 1, cell_val))
                    row_values.append(cell_val)
            if row_values and not disregard:
                rows.append(row_values)
        return rows

    def get_uploaded_file_columns(self, path: str, overwrite: bool = True):
        """
        Returns all the column headers from a workbook

        :param path: storage location of uploaded file
        :param overwrite: if the file path exists but the overwrite command is False, this will throw an error saying
            that explicit permission is required to overwrite.
        :return: rows of uploaded file data
        """
        if os.path.exists(path):
            if not overwrite:
                msg = "File at path {} is already present. Specify if override is required".format(path)
                print(msg)
                raise RuntimeError(msg)

        book = xlrd.open_workbook(path)
        budget_sheet = book.sheet_by_index(0)
        column_headers = budget_sheet.row_values(0)
        return column_headers

    def row_values(self, row_idx: int = 0, start_idx: int = 0, end_idx: int = None, sheet: Union[str, int] = None):
        """
        Wrapper around the xlrd's row_values method. Returns the values in the first row of an excel file.
        if no idx is provided, it will default to 0. By default it uses the current sheet of the open workbook
        of the instance!

        :param row_idx: what row to read -- starts at 0 by default (The first row)
        :param start_idx: the index corresponding to which column of the excel file you wish to read
        :param end_idx: the column to stop reading at
        :param sheet: what sheet to read the row from.
        """
        if not sheet:
            xl_sheet = self.current_sheet
        elif isinstance(sheet, str):
            xl_sheet = self.workbook.sheet_by_name(sheet)
        elif isinstance(sheet, int):
            xl_sheet = self.workbook.sheet_by_index(sheet)
        else:
            raise RuntimeError("Invalid sheet datatype passed into row values. sheet value: {}".format(sheet))
        return xl_sheet.row_values(row_idx, start_idx, end_idx)

    def close_workbook(self):
        print("Closing workbook {}".format(self.workbook))
        try:
            self.workbook.close()
        except Exception as e:
            print('Unable to close workbook. Not stopping execution though. Error: {}'.format(e))
        self.workbook = None
        self.current_sheet = None

    @staticmethod
    def is_datetime(param):
        accepted_formats = [
            '%Y-%m-%d %H:%M:%SZ',
            '%m/%d/%Y %H:%M%SZ',
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%m/%d',
            '%m-%d',
            '%m/%d/%Y %H:%M',
            '%m/%d/%Y %I:%M',
            '%Y-%m-%d %H:%M',
            '%Y-%m-%d %I:%M'
        ]
        bad_formats = [
            '%m-%Y-%d',
            '%m-%Y',
            '%m-%Y-%d %H:%M'
        ]
        satisfied_bad = False
        datetime_obj = None
        for accepted_format in accepted_formats:
            try:
                datetime_obj = datetime.strptime(param, accepted_format)
                return True, datetime_obj
            except:
                pass

        for bad_format in bad_formats:
            try:
                datetime.strptime(param, bad_format)
                satisfied_bad = True
            except:
                pass
        if satisfied_bad:
            return False, None
        return True, datetime_obj

    @staticmethod
    def _convert_instructions_to_formats(instructions: dict, row_idx: int, col_idx: int) -> Union[Formatter, None]:
        """
        Generates an XFStyle formatter for each instruction set so it can be picked up and applied while writing. An
        example of this dictionary would be:
        {
            "row": {
                0: {"text_color": "black", "cell_borders": ["left", "top"]}
            },
            "column": {
                2: {"font": "italics"}
            },
            "cell": {
                (0,0) : {"font": "bold", "bg_color": "sky-blue", "cell_str_format": "$##0" }
            }
        }
        This function will take cell specific instructions first, then take column specific instructions and then
        row specific instructions. The row specific instruction will be the BASE line instructions. So if, for example,
        `"row": {0: {"text_color": "black", "cell_borders": ["left", "top"]}}` and then
        `column: {0: {"font": "italics"}}` was given, the column instruction would be appended to the row instruction.
        If no row instruction was provided, then the column instruction would stand alone
        :param instructions: a dictionary of instructions where the keys are either "row", "column" or "cell"
            instruction.
        :param row_idx: The given row that a set of instruction may match. This must be provided since we use the row
            index and the column index to match any cell specific instructions and return the formatter object for that
        :param col_idx: the given column that a set of instructions may match. This must be provided since we use the
        row and column index to match any cell specific instructions and return the formatter object
        :return: a Formatter object. If Nothing applies or no/invalid instructions are supplied, it will return
            a formatter object which has the default style
        """
        if not instructions:
            return Formatter()
        row_idx = str(row_idx)
        col_idx = str(col_idx)
        cell = (row_idx, col_idx)

        instructions_copy = copy.deepcopy(instructions)

        if instructions.get('cell') and instructions['cell'].get(cell):
            total_instructions = instructions_copy['cell'].get(cell)
            if instructions.get('row') and instructions['row'].get(row_idx):
                total_instructions = instructions_copy['row'][row_idx]
                if instructions.get('column') and instructions['column'].get(col_idx):
                    for k, v in instructions_copy['column'][col_idx].items():
                        total_instructions[k] = v
            else:
                if instructions.get('column') and instructions['column'].get(col_idx):
                    total_instructions = instructions_copy['column'][col_idx]
            for k, v in instructions_copy['cell'][cell].items():
                total_instructions[k] = v  # this will either add new k/v to instructions or replace some
            return Formatter(**total_instructions)

        if instructions.get('row') and instructions['row'].get(row_idx):
            total_instructions = instructions_copy['row'][row_idx]
            if instructions.get('column') and instructions['column'].get(col_idx):
                for k, v in instructions_copy['column'][col_idx].items():
                    total_instructions[k] = v
        elif instructions.get('column') and instructions['column'].get(col_idx):
            total_instructions = instructions_copy['column'][col_idx]
        else:
            return Formatter()
        return Formatter(**total_instructions)

    @property
    def offset(self):
        return self._offset
class ParseLookup:
    def parse(self, input_data):
        typeProperties = input_data.get('typeProperties','')
        if typeProperties:
            source = typeProperties.get('source', '')
            if source:
                sproc_name = source.get('sqlReaderStoredProcedureName', '')
                param_details = []
                param_obj = source.get('storedProcedureParameters', '')
                for param in param_obj:
                    val = param_obj.get(param).get('value')
                    if type(val) is dict:
                        val = str(val.get('value'))
                    else:
                        val = str(val)
                    param_details.append(param + ': ' + val)

                return '{} ,    Parameters: {}'.format(sproc_name, ','.join(param_details))

class ParseIfCondition:
    def parse(self, input_data):
        typeProperties = input_data.get('typeProperties','')
        if typeProperties:
            expression = typeProperties.get('expression', '')
            if expression:
                exp_val = expression.get('value','')
                return 'Expression:     {}'.format(exp_val)

class ParseSPROC:
    def parse(self, input_data):
        typeProperties = input_data.get('typeProperties','')
        if typeProperties:
            sproc_name = typeProperties.get('storedProcedureName', '')
            param_details = []
            param_obj = typeProperties.get('storedProcedureParameters', '')
            for param in param_obj:
                val = param_obj.get(param).get('value')
                if type(val) is dict:
                    val = str(val.get('value'))
                else:
                    val = str(val)
                param_details.append(param + ': ' + val)

            return 'Stored procedure name {0} ,    Parameters: {1}'.format(sproc_name, ','.join(param_details))

class ParseWebActivity:
    def parse(self, input_data):
        typeProperties = input_data.get('typeProperties', '')
        if typeProperties:
            url_name = typeProperties.get('url', '')
            method_name = typeProperties.get('method', '')
            header_name = typeProperties.get('header', '')
            if header_name:
                        content = header_name.get('Content-Type','')
                        print(content)
            Body_properties = typeProperties.get('body', '')


        return 'Http request URL: {0} \n\n Method Name : {1} \n\n Body properties : {2}'.format(url_name,method_name,Body_properties)

class ParseWaitActivity:
    def parse(self, input_data):
        typeProperties = input_data.get('typeProperties', '')
        if typeProperties:
           wait_val = typeProperties.get('waitTimeInSeconds','')
           return 'Wait time in Seconds:  {}'.format(wait_val)

class ParseDeleteActivity:
    def parse(self, input_data):
        global val
        typeProperties = input_data.get('typeProperties', '')
        if typeProperties:
            dataset_name = typeProperties.get('dataset','')
            reference_Name = dataset_name.get('referenceName','')
            store_settings = typeProperties.get('storeSettings','')
            if store_settings:
                wildcardFileName = store_settings.get('wildcardFileName','')

                return 'DataSet name-{0} \nFileName-{1}'.format(reference_Name,wildcardFileName)

class ParseExecutePipeline:
    def parse(self, input_data):
        typeProperties = input_data.get('typeProperties','')
        if typeProperties:
            pipeline = typeProperties.get('pipeline', '')
            if pipeline:
                ref = pipeline.get('referenceName','')
                return 'Child pipeline Name : {}'.format(ref)

class ParseCopyActivity:
    def parse(self, input_data):
        typeProperties = input_data.get('typeProperties', '')

        if input_data.get('inputs', ''):
            inputs = input_data.get('inputs', '')
            inputreferenceName = inputs[0].get('referenceName', '')

        if input_data.get('outputs', ''):
            outputs = input_data.get('outputs', '')
            outputreferenceName = outputs[0].get('referenceName', '')

        if typeProperties:
            source = typeProperties.get('source', '')
            if source:
                type = source.get('type', '')
                if type != 'SqlDWSink':
                    if source.get('storeSettings', ''):
                        storeSettings = source.get('storeSettings', '')
                        if storeSettings.get('wildcardFileName', ''):
                            v_source = storeSettings.get('wildcardFileName', '')
                            v_foldername = storeSettings.get('wildcardFolderPath', '')

                            return 'Input DataSet : {0} \nOutput DataSet : {1}\n File name : {2}\nFolder Name or Path:{3}'.format(
                                inputreferenceName, outputreferenceName, v_source, v_foldername)

                if type == 'SqlDWSource':
                    if source.get('sqlReaderStoredProcedureName', ''):
                        v_source = source.get('sqlReaderStoredProcedureName', '')
                        v_foldername = ''

                        return 'Input DataSet : {0} \nOutput DataSet : {1}\n File name : {2}\nFolder Name or Path:{3}'.format(
                            inputreferenceName, outputreferenceName, v_source, v_foldername)

class ParseNotebookActivity:
    def parse(self, input_data):
        typeProperties = input_data.get('typeProperties', '')
        if typeProperties:
            notebook_path = typeProperties.get('notebookPath', '')
            param_details = []
            param_obj = typeProperties.get('baseParameters', '')
            for param in param_obj:
                val = param_obj.get(param)  # .get('value')
                if type(val) is dict:
                    val = str(val.get('value'))
                    val = param + ' : ' + val
                else:
                    val = str(param + ' : ' + val)
                param_details.append(val)

            param = ""

            for i in param_details:
                param = i + ',\n' + param


        return 'Notebook path: {0} \n\n Paramerters: {1}'.format(notebook_path,param)

class ParseGetMetadataActivity:
    def parse(self, input_data):
        typeProperties = input_data.get('typeProperties', '')
        if typeProperties:
            ds = typeProperties.get('dataset')
            dataset_name = ds.get('referenceName', '')
            attr_obj = typeProperties.get('fieldList', '')
            attr = ""
            for i in attr_obj:
                attr = i + ' , ' + attr
            return 'Dataset name: {0} \n\n Metadata attributes: {1}'.format(dataset_name,attr)

parse_lookup = ParseLookup()
parse_ifcondition = ParseIfCondition()
parse_sproc = ParseSPROC()
parse_webactivity = ParseWebActivity()
parse_waitactivity = ParseWaitActivity()
parse_deleteactivity = ParseDeleteActivity()
parse_executepipeline = ParseExecutePipeline()
parse_copyactivity = ParseCopyActivity()
parse_notebook = ParseNotebookActivity()
parse_getmetadata = ParseGetMetadataActivity()

class ADFPipelineDocGenerator:

    def __init__(self):
        self.pipeline_name_flag = True
        self.pipeline_name = ''
        self.table_data = []
    def recursive_parsing_Individual(self, input_data, parent_task_name):
        current_task_name = None
        for data in input_data:
            if type(input_data) is dict:
                if type(input_data.get(data, 0)) is str:
                    if data == 'name':
                        if self.pipeline_name_flag:
                            self.pipeline_name_flag = False
                            self.pipeline_name = input_data.get('name', '')

                        else:
                            current_task_name = input_data.get('name', '')
                            task_type = input_data.get('type', '')
                            tasks = ['SqlServerStoredProcedure', 'Lookup', 'IfCondition', 'GetMetadata',
                                     'DatabricksNotebook', 'WebActivity', 'Wait', 'Delete', 'Copy']
                            if any(x in task_type for x in tasks):
                                task_details = self.parse_task_details(task_type, input_data) or ''
                                task_dependency_info = self.parse_dependsOn(
                                    input_data.get('dependsOn', '')) or parent_task_name

                                self.table_data.append([self.pipeline_name, current_task_name, task_type, task_details,
                                                        task_dependency_info])


                elif type(input_data.get(data, 0)) is dict:
                    self.recursive_parsing_Individual(input_data[data], current_task_name or parent_task_name)
                elif type(input_data.get(data, 0)) is list:
                    self.recursive_parsing_Individual(input_data[data], current_task_name or parent_task_name)
            elif type(data) is dict:
                self.recursive_parsing_Individual(data, current_task_name or parent_task_name)

    def recursive_parsing(self, input_data, parent_task_name):
        current_task_name = None
        for data in input_data:
            if type(input_data) is dict:
                if type(input_data.get(data, 0)) is str:
                    if data == 'name':
                            v_type = "[concat(parameters('factoryName')"
                            if v_type in (input_data.get(data)):
                                pipeline_name  = input_data.get('name', '')
                                end_idx = pipeline_name.rfind("'")
                                self.pipeline_name = (pipeline_name[37:end_idx])
                            else:
                                current_task_name = input_data.get('name', '')
                                task_type = input_data.get('type', '')
                                tasks = ['SqlServerStoredProcedure', 'Lookup', 'IfCondition','GetMetadata','DatabricksNotebook','WebActivity','Wait','Delete','Copy']
                                if any(x in task_type for x in tasks):
                                    task_details = self.parse_task_details(task_type, input_data) or ''
                                    task_dependency_info = self.parse_dependsOn(
                                    input_data.get('dependsOn', '')) or parent_task_name

                                    self.table_data.append([self.pipeline_name, current_task_name, task_type, task_details,
                                                        task_dependency_info])

                elif type(input_data.get(data, 0)) is dict:
                    self.recursive_parsing(input_data[data], current_task_name or parent_task_name)
                elif type(input_data.get(data, 0)) is list:
                    self.recursive_parsing(input_data[data], current_task_name or parent_task_name)
            elif type(data) is dict:
                self.recursive_parsing(data, current_task_name or parent_task_name)

    def parse_dependsOn(self, dependency_list):
        if dependency_list:
            parent_task_name = []
            for obj in dependency_list:
                if type(obj) is dict:
                    parent_task_name.append(obj.get('activity','') + ':' + ','.join(obj.get('dependencyConditions',[])))

            return ','.join(parent_task_name)
        else:
            return None

    def parse_task_details(self, task_type, obj):
            if task_type == 'Lookup':
                return parse_lookup.parse(obj)
            elif task_type == 'IfCondition':
                return parse_ifcondition.parse(obj)
            elif task_type == 'SqlServerStoredProcedure':
                return parse_sproc.parse(obj)
            elif task_type == 'WebActivity':
                return parse_webactivity.parse(obj)
            elif task_type == 'Wait':
                return parse_waitactivity.parse(obj)
            elif task_type == 'Delete':
                return parse_deleteactivity.parse(obj)
            elif task_type == 'ExecutePipeline':
                return parse_executepipeline.parse(obj)
            elif task_type == 'Copy':
                return parse_copyactivity.parse(obj)
            elif task_type == 'DatabricksNotebook':
                return parse_notebook.parse(obj)
            elif task_type == 'GetMetadata':
                return parse_getmetadata.parse(obj)

doc_gen = ADFPipelineDocGenerator()

def get_initial_row_formatting(initial_row):
    # format the excel
    uniform_format = {
        "font_styles": ["bold"],
        "text_color": "black",
        "bg_color": "#99CCFF",
        "cell_borders": ["top"]
    }

    initial_row_instruction = {"column": {}}

    for i in range(len(initial_row)):
        initial_row_instruction["column"][str(i)]=uniform_format.copy()
        if i == len(initial_row) - 1:
            initial_row_instruction["column"][str(i)]["cell_borders"] = ["top", "right"]
    return initial_row_instruction

def get_rest_rows_formatting(start_position, formatted_row):
    # format the excel
    rest_rows_instruction = {"column": {}}

    # change cell_str_format
    for i in range(start_position, len(formatted_row)):
        rest_rows_instruction['column'][str(i)] = {}
        if i != 3:
            rest_rows_instruction['column'][str(i)]['cell_str_format'] = \
                '[$$-409]#,##0_);[$$-409](#,##0)'
        if i == len(formatted_row)-1:
            rest_rows_instruction['column'][str(i)]["cell_borders"]= ["right"]
    return rest_rows_instruction

file = sys.argv[1]
with open(file) as json_data_file:
    json_data = json.load(json_data_file)
    if type(json_data.get('resources', '')) is list:
        resource_obj = json_data.get('resources', '')
        for data in resource_obj:
            if data.get('type') == "Microsoft.DataFactory/factories/pipelines":
                doc_gen.recursive_parsing(data, '')
    else:
        doc_gen.recursive_parsing_Individual(json_data, '')

    xl_utility = ExcelUtils()
    now = datetime.now()
    formatted_date = now.strftime("%Y-%m-%d-%H-%M-%S-%f")[:-3]
    file_name = '{}_{}.xlsx'.format(file[:-5], formatted_date)
    temp_path = os.path.join(pathlib.Path().absolute(), file_name)

    headers = ['Pipeline Name', 'Task Name', 'Type', 'Details', 'Depency Task: Condition']
    initial_row_formatting = get_initial_row_formatting(headers)
    xl_utility.create_new(temp_path, [headers], overwrite=True, instructions=initial_row_formatting)
    for j, row in enumerate(doc_gen.table_data):
        formatted_row = [''] * len(headers)
        each_row_formatting = get_rest_rows_formatting(1, formatted_row)
        xl_utility.write_row(row, instructions=each_row_formatting)
    xl_utility.close_workbook()

