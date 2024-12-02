import boto3
import yaml
import pandas as pd
import streamlit as st
import os
import io
import json
import plotly.graph_objects as go
import plotly.express as px


def load_aws_credentials(yaml_file):
    try:
        with open(yaml_file, "r") as file:
            config = yaml.safe_load(file)
        aws_config = config.get("aws", {})
        if not all(k in aws_config for k in ("access_key_id", "secret_access_key", "region")):
            raise ValueError("Missing required AWS credentials in YAML file.")
        return aws_config
    except FileNotFoundError:
        raise FileNotFoundError(f"YAML file {yaml_file} not found.")
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file: {str(e)}")


def list_and_fetch_objects(bucket_name, prefix, aws_credentials):
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_credentials["access_key_id"],
            aws_secret_access_key=aws_credentials["secret_access_key"],
            region_name=aws_credentials["region"]
        )
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if "Contents" not in response:
            raise ValueError(f"No objects found in prefix '{prefix}' in bucket '{bucket_name}'.")

        files_data = []
        for obj in response["Contents"]:
            key = obj["Key"]
            print(f"Fetching object: {key}")
            obj_response = s3.get_object(Bucket=bucket_name, Key=key)
            file_data = obj_response["Body"].read().decode("utf-8")
            files_data.append((key, file_data))
        return files_data
    except Exception as e:
        raise RuntimeError(f"Error fetching objects from S3: {str(e)}")


def parse_data_quality_reports(files_data):
    all_reports = []
    for key, file_data in files_data:
        try:
            json_data = json.loads(file_data)

            # Extract date from the first element
            date = json_data['data_quality_report'][0].get('Date', 'Unknown Date')

            # Extract check data
            check_data = [check for check in json_data['data_quality_report'] if 'check_name' in check]

            # Add date to each check record
            for check in check_data:
                check['Date'] = date

            all_reports.extend(check_data)
        except json.JSONDecodeError as e:
            st.error(f"Could not parse {key} as JSON. Error: {e}")

    return pd.DataFrame(all_reports)


def create_comparative_visualizations(df):
    st.title("Data Quality Monitoring Dashboard")

    # Date selection sidebar
    st.sidebar.header("Date Filter")
    available_dates = sorted(df['Date'].unique())
    selected_dates = st.sidebar.multiselect(
        "Select Dates to Compare",
        available_dates,
        default=available_dates
    )

    # Filter data based on selected dates
    filtered_df = df[df['Date'].isin(selected_dates)]

    # Comparative Bar Chart
    st.subheader("Pass Rates Comparison")

    # Prepare data for plotting
    plot_df = filtered_df.pivot(index='Date', columns='check_name', values='passed').reset_index()

    # Create bar chart
    fig_bar = go.Figure()

    # Define check names and colors
    check_names = ['Consistency Issues', 'Duplicate Records', 'Missing Values']
    colors = ['#2ECC71', '#3498DB', '#E74C3C']

    # Add traces for each check type
    for check, color in zip(check_names, colors):
        if check in plot_df.columns:
            fig_bar.add_trace(go.Bar(
                name=check,
                x=plot_df['Date'],
                y=plot_df[check],
                marker_color=color,
                text=[f"{val:.1f}%" for val in plot_df[check]],
                textposition='auto'
            ))

    fig_bar.update_layout(
        barmode='group',
        height=500,
        title='Pass Rates Across Selected Dates',
        xaxis_title='Date',
        yaxis_title='Pass Rate (%)',
        legend_title='Check Type'
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    # Detailed Metrics Table
    st.subheader("Detailed Metrics")
    metrics_table = filtered_df.pivot_table(
        index='Date',
        columns='check_name',
        values=['passed', 'failed'],
        aggfunc='first'
    )
    st.dataframe(metrics_table)

    # Overall Summary
    st.subheader("Overall Summary")
    summary_cols = st.columns(3)

    # Average pass rate across selected dates
    avg_pass_rate = filtered_df.groupby('Date')['passed'].mean()
    summary_cols[0].metric(
        "Avg Pass Rate",
        f"{avg_pass_rate.mean():.1f}%"
    )

    # Number of checks passed
    checks_passed = filtered_df.groupby('Date')['threshold_met'].sum()
    total_checks = filtered_df.groupby('Date')['threshold_met'].count()
    summary_cols[1].metric(
        "Checks Passed",
        f"{checks_passed.sum()}/{total_checks.sum()}"
    )

    # Overall status
    overall_status = "✅ PASSED" if (checks_passed == total_checks).all() else "❌ FAILED"
    summary_cols[2].metric(
        "Overall Status",
        overall_status
    )


def main():
    st.set_page_config(layout="wide")

    yaml_file_path = "cred.yaml"
    bucket_name = "data-quality-monitoring-pyspark"
    prefix = "output/"

    try:
        aws_credentials = load_aws_credentials(yaml_file_path)
        files_data = list_and_fetch_objects(bucket_name, prefix, aws_credentials)

        # Parse all data quality reports
        df = parse_data_quality_reports(files_data)

        # Create visualizations
        create_comparative_visualizations(df)

    except Exception as e:
        st.error(f"Error: {e}")


if __name__ == "__main__":
    main()