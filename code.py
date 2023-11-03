import xlrd
import mysql.connector
import json
from collections import defaultdict

class Util:

    @staticmethod
    def connect_to_database(host, user, password, database):
        # connect to mysql
        db = mysql.connector.connect(host=host, user=user, password=password, database=database)
        return db, db.cursor()

    @staticmethod
    def read_excel(loc):
        # reading data from excel sheet
        wb = xlrd.open_workbook(loc)
        sheet = wb.sheet_by_index(0)
        return sheet

    # make the names in right format for mysql
    @staticmethod
    def format_column_name(name):
        result = name
        if "/" in result:
            result = result.replace("/", "_")
        if " " in result:
            result = result.replace(" ", "_")
        if ":" in result:
            result = result.replace(":", "")
        return result

    @staticmethod
    def format_table_name(name):
        result = name
        if "(" in result:
            if ")" in result:
                result = result.replace(")", "")
            result = result.replace("(", "__")
        if " " in result:
            result = result.replace(" ", "_")
        return result

    @staticmethod
    def find_attributes_excel(product_name, exsheet):
        result_excel = []   # will be containing single_valued attributes only
        multi_excel = []
        for row in range(1, exsheet.nrows):
            num_of_vals_for_tuple = {}
            if exsheet.cell(row, 5).value == product_name:
                if exsheet.cell(row, 9).value == '':
                    continue
                else:
                    dict_list = json.loads(exsheet.cell(row, 9).value)
                    for dict in dict_list:  # for each keyvalue pair
                        key = Util.format_column_name(dict['Key'])
                        # todo check if value is null ya stringe khali lazeme?
                        num_of_vals_for_tuple[key] = num_of_vals_for_tuple.get(key, 0) + 1
                        if key not in result_excel:
                            result_excel.append(key)
            for att in num_of_vals_for_tuple:  # for each attribute key
                if att not in multi_excel:
                    if num_of_vals_for_tuple[att] >= 2:
                        multi_excel.append(att)
        for i in multi_excel:
            result_excel.remove(i)
        return result_excel, multi_excel

    @staticmethod
    def find_attributes_sql(product_name, sh):
        result_sql = []
        multi_sql = []
        result_excel, multi_excel = Util.find_attributes_excel(product_name, sh)
        for i in range(0, len(result_excel)):
            result_sql.append(Util.format_column_name(result_excel[i]))
        for i in range(0, len(multi_excel)):
            multi_sql.append(Util.format_column_name(multi_excel[i]))
        return result_sql, multi_sql

    @staticmethod
    def create_database_in_mysql(host, user, password, database):
        db = mysql.connector.connect(host=host, user=user, password=password)
        db.cursor().execute("CREATE DATABASE " + database)


class Database:

    def __init__(self, host, user, password, database, list_of_categories):
        self.db, self.cursor = Util.connect_to_database(host, user, password, database)
        self.sheet = Util.read_excel("/data/5-awte8wbd.xlsx")
        self.tables = list_of_categories
        self.ex_string_columns = []  # is kasif but we set the in functions if they havnt been already set
        # TODO

    def my_initialize(self):
        self.get_string_columns_of_excel()

        # except for attributes
    def get_string_columns_of_excel(self):
        for i in range(1, 9):
            self.ex_string_columns.append(self.sheet.cell_value(0, i))

    def get_no_vio_str_cols(self):
        string_cols = []
        for i in range(0, 5):
            string_cols.append(self.ex_string_columns[i])
        string_cols.append(self.ex_string_columns[6])
        return string_cols

    def create_product_table(self, product_name):
        table_name = Util.format_table_name(product_name)
        last_str = self.get_no_vio_str_cols()
        # string_cols = self.get_string_cols()
        # tmp_1 = '  VARCHAR(255),'.join(string_cols)
        tmp = '  VARCHAR(255),'.join(last_str)
        attributes, unwanted = Util.find_attributes_sql(product_name, self.sheet)
        tmp_str = ' VARCHAR(255) DEFAULT NULL,'.join(attributes)

        self.cursor.execute("CREATE TABLE " + table_name +
                            "(product_id            FLOAT, " +
                            tmp + " VARCHAR(100), " + tmp_str + " TEXT DEFAULT NULL, " +
                            "PRIMARY KEY (product_id) );")
        self.db.commit()

    def load_all_products_except_attr(self, categroies):
        tt = []
        tt = self.get_no_vio_str_cols()
        tmp = ' ,'.join(tt)
        for r in range(0, self.sheet.nrows):
            cat = self.sheet.cell_value(r, 5)
            if cat in categroies:
                temp_tuple = []
                for c in range(0, 8):
                    if c != 6:
                        temp_tuple.append(self.sheet.cell_value(r, c))
                query = "INSERT INTO " + Util.format_table_name(cat) + " (product_id ," + tmp + ")" +\
                        "VALUES (%s, %s, %s, %s, %s, %s, %s)"
                values = (temp_tuple[0], temp_tuple[1], temp_tuple[2], temp_tuple[3], temp_tuple[4],
                          temp_tuple[5], temp_tuple[6])
                self.cursor.execute(query, values)
        self.db.commit()

    def find_keyword(self, cat):
        for r in range(1, self.sheet.nrows):
            if self.sheet.cell_value(r, 5) == cat:
                return self.sheet.cell_value(r, 6)

    def create_and_load_catkey_table(self, categories):
        self.cursor.execute("CREATE TABLE category_key (category_title_fa VARCHAR(100), category_keywords VARCHAR(100))")
        key_words = {}
        for i in categories:
            key_words[i] = self.find_keyword(i)
            temp = "'" + Util.format_table_name(i) + "'" + "," + "'" + key_words[i] + "'"
            self.cursor.execute("INSERT INTO category_key VALUES (" + temp + ")")
        self.db.commit()

    def create_and_load_multi_valued_tables(self, product_name):
        product_sql = Util.format_table_name(product_name)
        unwanted, multi = Util.find_attributes_sql(product_name, self.sheet)
        for i in multi:
            multi_table_name = i + "___" + product_sql
            self.cursor.execute("CREATE TABLE " + multi_table_name + " (number INT AUTO_INCREMENT, product_id FLOAT, " + i + " VARCHAR(255), PRIMARY KEY(number))")

        for r in range(0, self.sheet.nrows):
            if self.sheet.cell(r, 5).value == product_name and self.sheet.cell(r, 9).value != '':
                multi_dict = defaultdict(list)
                dict_list = json.loads(self.sheet.cell(r, 9).value)
                for dict in dict_list:
                    if 'Value' in dict and 'Key' in dict:
                        key = Util.format_column_name(dict['Key'])
                        if key in multi:
                            multi_dict[key].append(dict['Value'])
                for key in multi_dict:
                    for val in multi_dict[key]:
                        multi_table = key + "___" + product_sql
                        self.cursor.execute("INSERT INTO " + multi_table + "(product_id," + key + ") " + "VALUES (" + str(self.sheet.cell(r, 0).value) + ",'" + val +"' )")
        self.db.commit()


    def update_single_val_attributes(self, product):
        table_name = Util.format_table_name(product)
        for r in range(0, self.sheet.nrows):
            if self.sheet.cell(r, 5).value == product and self.sheet.cell(r, 9).value != '':
                single = {}
                multi = defaultdict(list)
                single_atts, multi_atts = Util.find_attributes_sql(product, self.sheet)
                dict_list = json.loads(self.sheet.cell(r, 9).value)
                for dict in dict_list:
                    if 'Value' in dict and 'Key' in dict:
                        key = Util.format_column_name(dict['Key'])
                        if key in single_atts:
                            single[key] = dict["Value"]
                        else:
                            if key in multi_atts:
                                multi[key].append(dict['Value'])
                single_key_vals = []
                for key in single:
                    single_key_vals.append(key + "=" + "'" + single[key] + "'")
                query = "UPDATE IGNORE " + table_name + " SET " + ",".join(single_key_vals) + "WHERE product_id=" + str(self.sheet.cell(r, 0).value)
                self.cursor.execute(query)

        self.db.commit()


    def close_connection(self):
        self.cursor.close()
        self.db.close()


def main():

    host = input("Enter host: ")
    user = input("Enter username: ")
    password = input("Enter password: ")
    database = "Digikala"

    Util.create_database_in_mysql(host, user, password, database)

    excel_categories = [u'کتاب چاپی', u'پازل', u'ماوس (موشواره)', u'کیبورد (صفحه کلید)',
                  u'محافظ صفحه نمایش گوشی', u'کیف و کاور گوشی']

    right_format_cats = []  # which are names of tables
    for i in range(0, len(excel_categories)):
        right_format_cats.append(Util.format_table_name(excel_categories[i]))

    database = Database(host, user, password, database, right_format_cats)
    database.my_initialize()
    for i in excel_categories:
        database.create_product_table(i)
    database.load_all_products_except_attr(excel_categories)
    for i in excel_categories:
        database.create_and_load_multi_valued_tables(i)
        database.update_single_val_attributes(i)
    database.create_and_load_catkey_table(excel_categories)
    database.close_connection()
    print("Database Digikala is created")

if __name__ == '__main__':
    main()
