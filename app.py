import os
import re
import time
from flask import Flask, request, jsonify
from pathlib import Path
import pandas as pd
import pandera as pa
from pandera import DataFrameSchema, Column, Check
import multiprocessing as mp
import concurrent.futures
from dask import dataframe as dd
from dask.distributed import Client, LocalCluster
import dask


schema = DataFrameSchema(
    {
        "EMPLOYER_ID": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=False),
        "CLAIM_STATUS": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "CLAIM_TYPE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "SERVICE_START_DATE": Column(pa.DateTime, nullable=True),
        "SERVICE_END_DATE": Column(pa.DateTime, nullable=True),
        "PROVIDER_NPI": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "PLACE_OF_SERVICE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "CPT_PROCEDURE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "DIAGNOSIS_1": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "CLAIM_PAID_DATE": Column(pa.DateTime, nullable=True),
        "COVERED_AMOUNT": Column(pa.Float, checks=[pa.Check(lambda x: x >= 0)], nullable=True),
        "PLAN_PAID_AMOUNT": Column(pa.Float, checks=[pa.Check(lambda x: x >= 0)], nullable=True),
        "PATIENT_SSN": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "INPATIENT_OR_OUTPATIENT": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "CLAIM_CAUSE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "BENEFIT_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "NETWORK": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "PROVIDER_NAME": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "PROVIDER_PAID_NAME": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "CHARGED_AMOUNT": Column(pa.Float, checks=[pa.Check(lambda x: x >= 0)], nullable=True),
        "UCR": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "CPT_MODIFIER": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "DIAGNOSIS_2": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "DIAGNOSIS_3": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "DIAGNOSIS_4": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "DIAGNOSIS_5": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "MEMBER_DEDUCTIBLE_AMOUNT": Column(pa.Float, checks=[pa.Check(lambda x: x >= 0)], nullable=True),
        "MEMBER_OOP_AMOUNT": Column(pa.Float, checks=[pa.Check(lambda x: x >= 0)], nullable=True),
        "MEMBER_COPAY_AMOUNT": Column(pa.Float, checks=[pa.Check(lambda x: x >= 0)], nullable=True),
        "CLAIM_NUMBER": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "CLAIM_RECEIVED_DATE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "CLAIM_ENTRY_DATE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "REMARKS_CODE_1": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "REMARKS_CODE_2": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "REMARKS_CODE_3": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "CHECK_NUMBER": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "BENEFITS_ASSIGNED": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "REVENUE_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "PROVIDER_EIN": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "PROVIDER_PAID_NPI": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "PROVIDER_PAID_ZIP": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "UNIQUE_PATIENT_ID": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_-]+$')], nullable=True),
        "LOCATION_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "SUB_GROUP_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "PLAN_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "ADMIT_DATE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "DISCHARGE_DATE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "ADMISSION_DAYS": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "DISCHARGE_STATUS_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "POINT_OF_ORIGIN_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "ADMISSION_DIAGNOSIS_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "PATIENT_REASON_DIAGNOSIS_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "CLAIM_FORM_TYPE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "TYPE_OF_BILL_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "ORIGINAL_PROCEDURE_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "ORIGINAL_POS_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "ORIGINAL_DIAGNOSIS_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "ORIGINAL_PROVIDER_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
    }
)


def date_format_parser(date_string):
    if isinstance(date_string, str):
        if '/' in date_string:
            return pd.to_datetime(date_string, format='%m/%d/%Y', errors='coerce')
        elif '-' in date_string:
            date_string = date_string.replace("-", "/")
            return pd.to_datetime(date_string, format='%m/%d/%Y', errors='coerce')
    return pd.NaT


def validate_chunk(chunk):
    print(f"Process {os.getpid()} started for chunk.")
    errors = []
    try:
        schema.validate(chunk, lazy=True)
    except pa.errors.SchemaErrors as e:
        for error in e.failure_cases.to_dict(orient='records'):
            errors.append({"index": error['index'], "column": error['column'], "error": error['failure_case']})
    print(f"Process {os.getpid()} finished for chunk.")
    return errors

# def validate_chunk(chunk):
#     print(f"Process {os.getpid()} started for chunk.")
#     errors = []
#     try:
#         schema.validate(chunk, lazy=True)
#     except pa.errors.SchemaErrors as e:
#         for i, error in enumerate(e.failure_cases.to_dict(orient='records')):
#             # Keep track of row number using enumerate
#             errors.append({"index": i, "column": error['column'], "error": error['failure_case'], "row_number": i})
#     print(f"Process {os.getpid()} finished for chunk.")
#     return errors


def create_app():
    app = Flask(__name__)

    @app.route('/validate_csv', methods=['POST'])
    def validate_csv():
        start_time = time.time()
        request_data = request.json
        file_path = request_data.get('file_path')

        if not file_path:
            return jsonify({"error": "File path not provided"}), 400

        path = Path(file_path)
        if not path.exists():
            return jsonify({"error": "File not found"}), 400

        try:
            print("Reading CSV file in parallel using Dask...")
            ddf = dd.read_csv(
                path,
                dtype={
                    'EMPLOYER_ID': 'category',
                    'EMPLOYER_NAME': 'category',
                    'CLAIM_STATUS': 'category',
                    'CLAIM_TYPE': 'category',
                    'PROVIDER_NPI': 'category',
                    'PLACE_OF_SERVICE': 'category',
                    'CPT_PROCEDURE': 'category',
                    'DIAGNOSIS_1': 'category',
                    'COVERED_AMOUNT': 'float64',
                    'PLAN_PAID_AMOUNT': 'float64',
                    'PATIENT_SSN': 'category',
                    'INPATIENT_OR_OUTPATIENT': 'category',
                    'CLAIM_CAUSE': 'category',
                    'BENEFIT_CODE': 'category',
                    'NETWORK': 'category',
                    'PROVIDER_NAME': 'category',
                    'PROVIDER_PAID_NAME': 'category',
                    'CHARGED_AMOUNT': 'float64',
                    'UCR': 'category',
                    'CPT_MODIFIER': 'category',
                    'DIAGNOSIS_2': 'category',
                    'DIAGNOSIS_3': 'category',
                    'DIAGNOSIS_4': 'category',
                    'DIAGNOSIS_5': 'category',
                    'MEMBER_DEDUCTIBLE_AMOUNT': 'float64',
                    'MEMBER_OOP_AMOUNT': 'float64',
                    'MEMBER_COPAY_AMOUNT': 'float64',
                    'CLAIM_NUMBER': 'category',
                    'CLAIM_RECEIVED_DATE': 'category',
                    'CLAIM_ENTRY_DATE': 'category',
                    'REMARKS_CODE_1': 'category',
                    'REMARKS_CODE_2': 'category',
                    'REMARKS_CODE_3': 'category',
                    'CHECK_NUMBER': 'category',
                    'BENEFITS_ASSIGNED': 'category',
                    'REVENUE_CODE': 'category',
                    'PROVIDER_EIN': 'category',
                    'PROVIDER_PAID_NPI': 'category',
                    'PROVIDER_PAID_ZIP': 'category',
                    'UNIQUE_PATIENT_ID': 'category',
                    'LOCATION_CODE': 'category',
                    'SUB_GROUP_CODE': 'category',
                    'PLAN_CODE': 'category',
                    'ADMIT_DATE': 'category',
                    'DISCHARGE_DATE': 'category',
                    'ADMISSION_DAYS': 'category',
                    'DISCHARGE_STATUS_CODE': 'category',
                    'POINT_OF_ORIGIN_CODE': 'category',
                    'ADMISSION_DIAGNOSIS_CODE': 'category',
                    'PATIENT_REASON_DIAGNOSIS_CODE': 'category',
                    'CLAIM_FORM_TYPE': 'category',
                    'TYPE_OF_BILL_CODE': 'category',
                    'ORIGINAL_PROCEDURE_CODE': 'category',
                    'ORIGINAL_POS_CODE': 'category',
                    'ORIGINAL_DIAGNOSIS_CODE': 'category',
                    'ORIGINAL_PROVIDER_CODE': 'category'
                },
                assume_missing=True,
                parse_dates=['SERVICE_START_DATE', 'SERVICE_END_DATE', 'CLAIM_PAID_DATE'],
                date_format=date_format_parser,
                blocksize=20e6
            )

            print("Converting Dask DataFrame to Pandas DataFrames for validation...")

            # Collecting all errors
            all_errors = []

            def process_partition(partition):
                df_errors = validate_chunk(partition)
                return df_errors

            # Apply process_partition to each partition
            partitions = ddf.to_delayed()
            futures = [dask.delayed(process_partition)(partition) for partition in partitions]
            results = dask.compute(*futures)

            for result in results:
                all_errors.extend(result)

            print("Processing finished.")

            if all_errors:
                response = {
                    "message": "Validation errors",
                    "errors": all_errors,
                    "time_taken": time.time() - start_time
                }
            else:
                response = {
                    "message": "Validation successful",
                    "time_taken": time.time() - start_time
                }

        except Exception as e:
            response = {
                "message": "Error processing file",
                "error": str(e),
                "time_taken": time.time() - start_time
            }

        return jsonify(response), 200

    return app


if __name__ == '__main__':
    import multiprocessing
    multiprocessing.freeze_support()
    app = create_app()
    app.run(debug=True)
