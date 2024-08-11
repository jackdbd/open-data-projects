# Ingestion

Data ingestion with [dlt](https://github.com/dlt-hub/dlt) pipelines.

Define `created_date_start` and `created_date_stop` in the `[sources.socrata]` section of `config.toml`, then run the dlt pipeline by typing `ingestion` (it's a [Devenv script](https://devenv.sh/scripts/)).

## Reference

- [Socrata Open Data API application tokens](https://dev.socrata.com/docs/app-tokens.html)
- [NYC Open Data Developer Settings](https://data.cityofnewyork.us/profile/edit/developer_settings) (manage your Socrata application tokens here)
- [Dataset "311 Service Requests from 2010 to Present" on NYC Open Data](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9/about_data)
- [Dataset "311 Service Requests from 2010 to Present" on dev.socrata.com](https://dev.socrata.com/foundry/data.cityofnewyork.us/erm2-nwe9)
- [Data dictionary for the dataset "311 Service Requests from 2010 to Present"](https://data.cityofnewyork.us/api/views/erm2-nwe9/files/b372b884-f86a-453b-ba16-1fe06ce9d212?download=true&filename=311_ServiceRequest_2010-Present_DataDictionary_Updated_2023.xlsx)
