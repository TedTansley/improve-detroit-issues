# Improve Detroit Reported Issues
Getting reported issues from Improve Detroit and sending them to a Google BigQuery Database and connecting it to Looker Studio Dashboard.

## Big Query Schema Table
| Field  | Data Type |
| ------------- | ------------- |
| ID  | INTEGER  |
| Status  | STRING  |
| Request_Type_Title  | STRING  |
| Report_Method  | STRING  |
| Created_At  | TIMESTAMP  |
| Acknowledged_At  | TIMESTAMP  |
| Closed_At  | TIMESTAMP  |
| Reopened_At  | TIMESTAMP  |
| Updated_At  | TIMESTAMP  |
| Days_to_Close  | FLOAT  |
| Address  | STRING  |
| Neighborhood  | STRING  |
| Council_District  | STRING  |
| Latitude  | BIGNUMERIC  |
| Longitude  | BIGNUMERIC  |
| Zip_Code  | STRING  |


## Looker Dashboard
https://lookerstudio.google.com/reporting/d3b7021b-98d9-42a6-893a-0d2476daec7a
### Key Features
- Overview Page
  - Case-sensitive search by address
    -  Allows users to find information about reported issues by entering in the address or street name
  -  Area search to narrow table by date, council district, city-defined neighborhood, and zip code
  -  Record/Report count
- District Pages with year over year KPIs and heatmap
  - Permits filtering by neighborhood, zip code, report status, and date range
- Null District page available to identify reported issues that did not receive City District information
