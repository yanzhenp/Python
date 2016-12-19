# -*- coding: utf-8 -*-


"""
    author: Arvin
"""

import argparse
import os
import configparser
import tempfile
import multiprocessing
import collections
import datetime

# ----------region multiprocessing----------
# code provided by chen-gang (@chen-gangh@hpe.com) modified by Arvin (@zhen-peng.yang@hpe.com)
Result = collections.namedtuple("Result", "schema_name table_name success failure time_in_minutes")
Summary = collections.namedtuple("Summary", "todo success failure total_time_in_minutes cancelled")


def worker(jobs):
    while True:
        try:
            (schema_name, table_name, param_src_sys_cd, constraint_type, constraint_name, reference_table_name,
             reference_keys, reference_column_name) = jobs.get()
            try:
                analyze(schema_name, table_name, param_src_sys_cd, constraint_type, constraint_name,
                        reference_table_name,
                        reference_keys, reference_column_name)
            except Exception as err:
                raise err
        finally:
            # MUST call task_done() method to let jobs.join() know ALL jobs are done
            jobs.task_done()


def add_jobs(schema_name, table_name, param_src_sys_cd, list_constraint_definition, jobs):
    for item in list_constraint_definition:
        if item != '':
            arr_itm = item.split('^')
            constraint_type = arr_itm[0]
            constraint_name = arr_itm[1]
            reference_table_name = arr_itm[2]
            reference_keys = arr_itm[3]
            reference_column_name = arr_itm[4]
            jobs.put((schema_name, table_name, param_src_sys_cd, constraint_type, constraint_name, reference_table_name,
                      reference_keys, reference_column_name))
    return len(list_constraint_definition)


def scale(schema_name, table_name, param_src_sys_cd, list_constraint_definition, concurrency):
    """ Run jobs in parallelism by which is defined in concurrency, and jobs are defined in table_list_file.
        Args:
        table_list_file(str): file name and path which defines all tables to be moved.
        concurrency(str): the number of jobs in parallel.
    Returns:
        namedTuple: "todo success failure total_time_in_minutes cancelled"
    """
    canceled = False
    jobs = multiprocessing.JoinableQueue()
    results = multiprocessing.Queue()
    create_processes(jobs, concurrency)
    todo = add_jobs(schema_name, table_name, param_src_sys_cd, list_constraint_definition, jobs)
    try:
        jobs.join()
    except KeyboardInterrupt:  # May not work on Windows
        canceled = True
    success = failures = total_time_in_minutes = 0
    while not results.empty():  # Safe because all jobs have finished
        result = results.get_nowait()
        success += result.success
        failures += result.failure
        total_time_in_minutes += result.time_in_minutes
    return Summary(todo, success, failures, total_time_in_minutes, canceled)


def create_processes(jobs, concurrency):
    """ Create OS process
        Args:
        jobs(Joinable object):
        results(object):
        concurrency(str): the number of jobs in parallel.
    Returns:
        namedTuple: "todo success failure total_time_in_minutes cancelled"
    """
    for _ in range(concurrency):
        process = multiprocessing.Process(target=worker, args=(jobs,))
        process.daemon = True
        # process.pid
        process.start()


# ----------------end region----------------


class ArgumentErrorException(Exception):
    def __init__(self, name):
        self.name = name


def create_tmp_file(query):
    """
    This function is to gegerate temporary file for query use
    :param query: query to be executed
    :return: query result as string
    """
    # print('---begin---')
    # print(query)
    # print('---end---')
    try:
        with tempfile.NamedTemporaryFile(mode='w+t', dir=os.getcwd(), prefix='analyze_constraint.tmp.',
                                         delete=False) as file:
            file.write(query)
        result = os.popen('hive --config hive-site.xml -S -f {0}'.format(file.name)).read()
    finally:
        file.close()
        os.remove(file.name)
    return result


def parse_args():
    """
    This function is to parse use entered arguments, of which -table and -asset is required, -key and -rfnc is optional
    :return:
        table_schema
        table_name
        asset_name
        key_type(optional)
        reference_table_name(optional)
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--table', help='table name to be analyzed', required=True, type=str)
    parser.add_argument('-s', '--asset', help='asset name the table belongs to', required=True, type=str)
    parser.add_argument('-k', '--key', help='specify constraint key type', required=False, type=str)
    parser.add_argument('-r', '--reference', help='specify constraint reference table', required=False, type=str)
    args = parser.parse_args()
    table = args.table
    asset_name = args.asset
    key_type = args.key
    reference_table_name = args.reference
    
    schema_name = ''
    table_name = ''
    if table.find('.') == -1:
        raise ArgumentErrorException(table)
    else:
        schema_name = table.split('.')[0]
        table_name = table.split('.')[1]
    return schema_name, table_name, asset_name, key_type, reference_table_name


def output_data(num_keys, data):
    """
    This function is to output issue data as an appropriate format
    :param num_keys:
    :param data:
    :return:
    """
    list_data = data.split('^')
    i = 0
    while i <= len(list_data) - num_keys:
        if i != len(list_data):
            if i == 0:
                print('Column Values   | {0}'.format(list_data[i:i + num_keys]))
            else:
                print('                | {0}'.format(list_data[i:i + num_keys]))
        else:
            print(list_data[i:-1])

        i += num_keys


def analyze(schema_name, table_name, param_src_sys_cd, constraint_type, constraint_name,
            reference_table_name,
            reference_keys,
            reference_column_name):
    """
    This function is to analyze specified constraint type and return error value if exists
    :param schema_name:
    :param table_name:
    :param param_src_sys_cd:
    :param constraint_type:
    :param constraint_name:
    :param reference_table_name:
    :param reference_keys:
    :param reference_column_name:
    :return: None
    """

    arr_reference_keys = reference_keys.split(',')

    if constraint_type == 'p':
        # f^FK1_ADDR^CTRY_CTY^CTRY_CTY_ID,SRC_SYS_CD^CTRY_CTY_ID,SRC_SYS_CD
        # need to separate fields and re-arrange
        pk_list_temp = ''
        for i_field in range(len(arr_reference_keys)):
            pk_list_temp += "nvl(cast (case when t.{0} is null then null else t.{0} end as string),'NULL')".format(
                arr_reference_keys[i_field])
            if i_field != len(arr_reference_keys) - 1:
                pk_list_temp += ','
        pk_list_formatted = "concat_ws('^', {0})".format(pk_list_temp)

        sql_check_pk = \
            ("select concat_ws('^', collect_set(A.s)) from ( select {0} as s from ( select {1} "
             "from {2}.{3} where SRC_SYS_CD='{4}' group by {1} "
             "having count(1) > 1 )t limit 10 )A").format(pk_list_formatted, reference_keys, schema_name, table_name,
                                                          param_src_sys_cd)
        res_check_pk = create_tmp_file(sql_check_pk)

        if res_check_pk.strip() == '':
            print('No PK issue found!')
        else:
            print('SQL:')
            print("\033[31m select {0} from {1}.{2} where SRC_SYS_CD='{3}' "
                  "group by {0} having count(1) > 1 )t limit 10 )A\n \033[0m".format(reference_keys, schema_name,
                                                                                     table_name, param_src_sys_cd))
            print("-[  RECORD  ]---+----------------\n"
                  "Schema Name     | {0}\n"
                  "Table Name      | {1}\n"
                  "Column Names    | {2}\n"
                  "Constraint Name | {3}\n"
                  "Constraint Type | PRIMARY\n"
                  "----------------+----------------".format(schema_name, table_name, reference_keys,
                                                               constraint_name))
            if res_check_pk.find('^') == -1:
                print("Column Values   | ('{0}')\n".format(res_check_pk))
            else:
                output_data(len(reference_keys.split(',')), res_check_pk)
    elif constraint_type == 'u':
        uk_list_temp = ''
        for i_field in range(len(arr_reference_keys)):
            uk_list_temp += "nvl(cast (case when t.{0} is null then null else t.{0} end as string),'NULL')".format(
                arr_reference_keys[i_field])
            if i_field != len(arr_reference_keys) - 1:
                uk_list_temp += ','
        uk_list_formatted = "concat_ws('^', {0})".format(uk_list_temp)

        sql_check_uk = \
            ("select concat_ws('^', collect_set(A.s)) from ( select {0} as s from ( select {1} "
             "from {2}.{3} where SRC_SYS_CD='{4}' group by {1} "
             "having count(1) > 1 )t limit 10 )A").format(uk_list_formatted, reference_keys, schema_name, table_name,
                                                          param_src_sys_cd)
        res_check_uk = create_tmp_file(sql_check_uk)

        if res_check_uk.strip() == '':
            print('No UK issue found!')
        else:
            print('SQL:')
            print("\033[31m select {0} from {1}.{2} where SRC_SYS_CD='{3}' "
                  "group by {0} having count(1) > 1 )t limit 10 )A\n \033[0m".format(reference_keys, schema_name,
                                                                                     table_name, param_src_sys_cd))
            print("-[  RECORD  ]---+----------------\n"
                  "Schema Name     | {0}\n"
                  "Table Name      | {1}\n"
                  "Column Names    | {2}\n"
                  "Constraint Name | {3}\n"
                  "Constraint Type | UNIQUE\n"
                  "----------------+----------------".format(schema_name, table_name, reference_keys,
                                                               constraint_name))
            if res_check_uk.find('^') == -1:
                print("Column Values   | ('{0}')\n".format(res_check_uk))
            else:
                output_data(len(reference_keys.split(',')), res_check_uk)
    elif constraint_type == 'n':
        nk_list_temp = ''
        null_condition = ''
        for i_field in range(len(arr_reference_keys)):
            null_condition = '{0}.{1} is null'.format(table_name, arr_reference_keys[i_field])
            if arr_reference_keys[i_field] == 'RADAR_UPD_TS':
                nk_list_temp += "case when RADAR_UPD_TS is null then 'NULL' else from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') end"
            else:
                nk_list_temp += "nvl(cast (case when {0} is null then null else {0} end as string),'NULL')".format(
                    arr_reference_keys[i_field])
            if i_field != len(arr_reference_keys) - 1:
                nk_list_temp += ','
                null_condition += ' or '
        nk_list_formatted = "concat_ws('^', {0})".format(nk_list_temp)

        sql_check_nk = \
            ("select concat_ws('^', collect_set(A.s)) from ( select {0} as s "
             "from {1}.{2} where SRC_SYS_CD='{3}' and ( {4} ) limit 10 )A").format(nk_list_formatted, schema_name,
                                                                                 table_name,
                                                                                 param_src_sys_cd, null_condition)
        res_check_nk = create_tmp_file(sql_check_nk)

        if res_check_nk.strip() == '':
            print('No Null issue found!')
        else:
            print('SQL:')
            print("\033[31m select {0} from {1}.{2} where SRC_SYS_CD='{3}' and {4}\n \033[0m".format(nk_list_formatted,
                                                                                                     schema_name,
                                                                                                     table_name,
                                                                                                     param_src_sys_cd,
                                                                                                     null_condition))
            print("-[  RECORD  ]---+----------------\n"
                  "Schema Name     | {0}\n"
                  "Table Name      | {1}\n"
                  "Column Names    | {2}\n"
                  "Constraint Name | {3}\n"
                  "Constraint Type | NULL\n"
                  "----------------+----------------".format(schema_name, table_name, reference_keys,
                                                               constraint_name))
            if res_check_nk.find('^') == -1:
                print("Column Values   | ('{0}')\n".format(res_check_nk))
            else:
                output_data(len(reference_keys.split(',')), res_check_nk)
    elif constraint_type == 'f':
        if table_name.upper() == reference_table_name.upper():
            print("Expression returned a reference to target table itself, skipping...")
        else:
            fk_list_temp = ''
            join_condition = ''
            null_condition = ''
            source_column_name = ''
            rfnc_column_name = ''
            for i_field in range(len(arr_reference_keys)):
                join_condition += "{0}.{1}={2}.{3}".format(table_name, arr_reference_keys[i_field],
                                                           reference_table_name,
                                                           reference_column_name.split(',')[i_field])
                null_condition += '{0}.{1} is not null'.format(table_name, arr_reference_keys[i_field])
                source_column_name += "{0}.{1}".format(table_name, arr_reference_keys[i_field])
                rfnc_column_name += "{0}.{1}".format(reference_table_name, reference_column_name.split(',')[i_field])
                fk_list_temp += "nvl(cast (case when t.{0} is null then null else t.{0} end as string),'NULL')".format(
                    arr_reference_keys[i_field])
                if i_field != len(arr_reference_keys) - 1:
                    fk_list_temp += ','
                    join_condition += ' and '
                    null_condition += ' and '
                    source_column_name += ','
                    rfnc_column_name += ','
            fk_list_formatted = "concat_ws('^', {0})".format(fk_list_temp)

            sql_check_fk = \
                ("select concat_ws('^', collect_set(A.s)) from ( select {0} as s from ( "
                 "select distinct {1} from {2}.{3} where not exists ( select distinct {4} from {2}.{5} "
                 "where {5}.SRC_SYS_CD='{6}' and {7} ) "
                 " and {8} and {3}.SRC_SYS_CD='{6}' )t limit 10 )A").format(fk_list_formatted, source_column_name,
                                                                            schema_name, table_name, rfnc_column_name,
                                                                            reference_table_name,
                                                                            param_src_sys_cd, join_condition,
                                                                            null_condition)
            res_check_fk = create_tmp_file(sql_check_fk)

            if res_check_fk.strip() == '':
                print('No FK issue found for reference table {0}! FK: {1}.'.format(reference_table_name, reference_keys))
            else:
                print('SQL:')
                print("\033[31m select distinct {0} from {1}.{2} where not exists ( select distinct {3} from {1}.{4} "
                      "where {4}.SRC_SYS_CD='{5}' and {6} ) "
                      " and {7} and {2}.SRC_SYS_CD='{5}' limit 10\n \033[0m").format(source_column_name,
                                                                                           schema_name, table_name,
                                                                                           rfnc_column_name,
                                                                                           reference_table_name,
                                                                                           param_src_sys_cd,
                                                                                           join_condition,
                                                                                           null_condition)
                print("-[  RECORD  ]---+----------------\n"
                      "Schema Name     | {0}\n"
                      "Table Name      | {1}\n"
		      "Reference Table | {2}\n"
                      "Column Names    | {3}\n"
                      "Constraint Name | {4}\n"
                      "Constraint Type | FOREIGN\n"
                      "----------------+----------------".format(schema_name, table_name, reference_table_name, reference_keys,
                                                                   constraint_name))
                if res_check_fk.find('^') == -1:
                    print("Column Values   | ('{0}')\n".format(res_check_fk))
                else:
                    output_data(len(reference_keys.split(',')), res_check_fk)
    else:
        pass


def main():
	  try:
		    # set work directory as home
		    start_time = datetime.datetime.now()
		    os.chdir('/home/radamgr')
		    try:
		    	  schema_name, table_name, asset_name, key_type, reference_table_name = parse_args()
		    except ArgumentErrorException as e:
		    	  print('Error: {0} is not a valid table name in search path, please check your input!'.format(e.name))
		    else:
				    try:
				        config = configparser.ConfigParser()
				        config.read('/home/radamgr/apps/Code/sh/radar_hadoop.env')
				        param_src_sys_cd = config.get(asset_name.upper(), 'Param_SRC_SYS_CD')
				    except configparser.NoSectionError:
				        print('Error: no section: \'{0}\''.format(asset_name.upper()))

				    if key_type is None:
				        key_condition = ''
				    else:
				        key_condition = " and upper(constraint_type) = '{0}'".format(str(key_type).upper())

				    if reference_table_name is None:
				        reference_condition = ''
				    else:
				        reference_condition = " and upper(reference_table_name) = '{0}'".format(str(reference_table_name).upper())

				        # need to find the constraint definition from radar.constraint_columns
				    print('*'*50)
				    print('\033[5m looking up constraint definitions... \033[0m')
				    print('*'*50)
				    sql_constraint_definition = \
				        ("select concat_ws('^', constraint_type,\n"
				         "constraint_name, nvl(cast (case when reference_table_name is null then null else reference_table_name end as string),'NULL'),\n"
				         "concat_ws(',', collect_set(column_name)),\n"
				         "nvl(cast (case when concat_ws(',',collect_set(reference_column_name)) is null then null else concat_ws(',',collect_set(reference_column_name)) end as string),\n"
				         "'NULL')) from radar.constraint_columns where upper(table_schema)='{0}' and upper(table_name)='{1}'{2}{3}\n"
				         "group by constraint_type, constraint_name, reference_table_name;").format(schema_name.upper(),
				                                                                                    table_name.upper(), key_condition,
				                                                                                    reference_condition)

				    res_constraint_definition = create_tmp_file(sql_constraint_definition)
				    # need to split string to list, the number of elements would be constraint num
				    list_constraint_definition = res_constraint_definition.split('\n')
				    # traverse the list
				    print('*'*50)
				    print('\033[5m analyzing... \033[0m')
				    print('*'*50)
				    concurrency = 3 if len(list_constraint_definition) >= 3 else len(list_constraint_definition)
				    scale(schema_name, table_name, param_src_sys_cd, list_constraint_definition, concurrency)
				    run_time = (datetime.datetime.now()-start_time).seconds
				    hour = run_time // 3600
				    minute = (run_time - hour * 3600) // 60
				    second = run_time % 60
				    print('{0} hours {1} minutes {2} seconds spent on the constraint checking.'.format(hour, minute, second))
	  except KeyboardInterrupt:
	  	  print('Error: KeyboardInterrupt, user pressed Ctrl+C!')


if __name__ == '__main__':
    main()


