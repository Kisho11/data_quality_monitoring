Clone the Repository
The complete codebase for this project is hosted on GitHub. You can clone the repository and use it to set up the Data Quality Monitoring Dashboard locally or on a server.

Repository URL: https://github.com/KNithushan/data_quality_monitoring

Cloning Instructions:
git clone https://github.com/KNithushan/data_quality_monitoring.git
cd data_quality_monitoring


AWS Credentials for Access
To ensure secure access to AWS resources like S3, Glue, and SNS, you need AWS credentials. If you do not already have the required access keys, please reach out via email.
AWS Credentials YAML File:
aws:
  access_key_id: "*******"
  secret_access_key: "********"
  region: "ap-southeast-2"

Contact: For AWS credentials, please contact us 
Nithushan knithushan@virtusa.com
Brindha brindhavenkatasamy@virtusa.com
Kalyan rkalyankumar@virtusa.com


Library Requirements
To implement and run the data quality checks and the dashboard, the following Python libraries are required:
•pandas: For data manipulation and analysis.
python -m pip install pandas
•Streamlit: For creating the interactive dashboard.
python -m pip install streamlit
•plotly: For generating interactive charts.
•Json: Results of data quality checks, making it easier to track and analyze data over time.
•boto3: For interacting with AWS services such as S3 for data storage and SNS notifications
python -m pip install boto3

