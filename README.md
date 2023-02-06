# safety-data-migration-script

This script is intended to be of one time use to migrate ~8k terminated employees' safety award data to a Cosmos DB container. The current data lives on a very old SharePoint server that does not have any migration tools available to get the data in Azure. The log is as follows:

- open data.xlsx (this file will be exported from the old SharePoint server)
- read the data and create a list of terminated employees
- generate JSON data from this list of terminated employees
- send to CosmosDB.
