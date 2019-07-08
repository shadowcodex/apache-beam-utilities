import argparse
import logging
import re
import datetime

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

import apache_beam_utilities as beam_utilities


def debug_pcollection(my_list_item):
    print(my_list_item)
    print("\n\n")


def setup_pipeline(argv=None):
    """Main entry point; defines and runs the pipeline."""
    parser = argparse.ArgumentParser()
    # parser.add_argument('--input',
    #                     dest='input',
    #                     default='gs://some/bucket/string....',
    #                     help='Input file to process.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)
    return (p, known_args)

def run(argv=None):
    dt = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    p, known_args = setup_pipeline(argv)

    # faked data for examples only.
    data_1 = (
        {
            "emp_id":"OrgEmp#1",
            "emp_name":"Jane",
            "emp_dept":"OrgDept#2",
            "emp_country":"USA",
            "emp_gender":"female",
            "emp_birth_year":"1980",
            "emp_salary":"$100000"
        },
        {
            "emp_id":"OrgEmp#2",
            "emp_name":"Scott",
            "emp_dept":"OrgDept#3",
            "emp_country":"USA",
            "emp_gender":"male",
            "emp_birth_year":"1985",
            "emp_salary":"$75000"
        },
        {
            "emp_id":"OrgEmp#3",
            "emp_name":"Lin",
            "emp_dept":"OrgDept#4",
            "emp_country":"USA",
            "emp_gender":"binary",
            "emp_birth_year":"1993",
            "emp_salary":"$13000"
        }
    )

    data_2 = (
        {
            "dept_id":"OrgDept#1",
            "dept_name":"Account",
            "dept_start_year":"1950"
        },
        {
            "dept_id":"OrgDept#2",
            "dept_name":"IT",
            "dept_start_year":"1990"
        },
        {
            "dept_id":"OrgDept#3",
            "dept_name":"HR",
            "dept_start_year":"1950"
        },
    )

    p_data_1 = p | "create data 1" >> beam.Create(data_1)
    p_data_2 = p | "create data 2" >> beam.Create(data_2)

    selected_1 = p_data_1 | "select p_data_1 on emp id/salary" >> beam_utilities.Select("emp_id,emp_salary")
    selected_1_none_fillNull = p_data_1 | "select p_data_1 on emp id/salary/phone" >> beam_utilities.Select("emp_id,emp_salary,emp_phone")
    selected_1_default_fillNull = p_data_1 | "select p_data_1 on emp id/salary/phone default" >> beam_utilities.Select("emp_id,emp_salary,emp_phone", True)
    selected_1_defined_fillNull = p_data_1 | "select p_data_1 on emp id/salary/phone defined" >> beam_utilities.Select("emp_id,emp_salary,emp_phone", True, "*")
    keyed_1 = p_data_1 | "set key emp dept" >> beam_utilities.PrepareKey("emp_dept")
    keyed_2 = p_data_2 | "set key dep id" >> beam_utilities.PrepareKey("dept_id")
    innerJoined = keyed_1 | "inner join" >> beam_utilities.InnerJoin(keyed_2)
    leftOuterJoined = keyed_1 | "left outer join" >> beam_utilities.LeftOuterJoin(keyed_2)
    rightOuterJoined = keyed_1 | "right outer join" >> beam_utilities.RightOuterJoin(keyed_2)
    fullOuterJoined = keyed_1 | "full outer join" >> beam_utilities.FullOuterJoin(keyed_2)

    selected_1 | 'writeSelect' >> beam.io.WriteToText("io/selected")
    selected_1_none_fillNull | 'writeSelected_1_none_fillNull' >> beam.io.WriteToText("io/selectedNoneFillNull")
    selected_1_default_fillNull | 'writeSelected_1_default_fillNull' >> beam.io.WriteToText("io/selectedDefaultFillNull")
    selected_1_defined_fillNull | 'writeSelected_1_defined_fillNull' >> beam.io.WriteToText("io/selectedDefinedFillNull")
    keyed_1  | 'writeKey1' >> beam.io.WriteToText("io/keyed_1")
    keyed_2  | 'writeKey2' >> beam.io.WriteToText("io/keyed_2")
    innerJoined | 'writeInnerJoin' >> beam.io.WriteToText("io/innerJoined")
    leftOuterJoined | 'writeLeftOuterJoin' >> beam.io.WriteToText("io/leftOuterJoined")
    rightOuterJoined | 'writeRightOuterJoin' >> beam.io.WriteToText("io/rightOuterJoined")
    fullOuterJoined | 'writeFullOuterJoin' >> beam.io.WriteToText("io/fullOuterJoined")


    ## Example SQL Query as Beam Pipeline
    ## SELECT emp_id, emp_salary, dept_name FROM data_1 LEFT OUTER JOIN data_2 on data_1.emp_dept = data_2.dept_id
    prepData_1 = p_data_1 | 'prepare key left' >> beam_utilities.PrepareKey("emp_dept")
    prepData_2 = p_data_2 | 'prepare key right' >> beam_utilities.PrepareKey("dept_id")
    queryResults = (
                        prepData_1 
                        | 'left outer join query' >> beam_utilities.LeftOuterJoin(prepData_2)
                        | 'select emp_id, emp_salary, dept_name' >> beam_utilities.Select("emp_id,emp_salary,dept_name", True, ''))

    queryResults | 'writeQueryResults' >> beam.io.WriteToText("io/queryResults")


    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()