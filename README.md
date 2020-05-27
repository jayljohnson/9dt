9dt take home project 
----------------------------------------------------------------- 
# Design
## Data Pipeline and load tables
### Assumptions
* The extract-load-transform pattern is preferred over extract-transform-load.  
* The tables in this section are used as a source for data warehouse loads but are not exposed to end users.
* All attributes in the games data is exposed to data warehouse end users.
* Only required attributes about users/players is exposed to data warehouse end users.
### Implementation details 
1. The player data from the users API is saved to S3 in the original JSON format. Each page of results contains 10 JSON documents, and each document is split into a new line before saving to S3.
2. The Athena table "dev9dt_athena.players" maps the JSON keys to table columns
3. The games data was manually copied from the http location to another S3 location accessable from Athena
4. The Athena table "dev9dt_athena.games" exposes the csv data to table columns
5. The Redshift external schema "athena_schema" is implemented from the Athena database "dev9dt_athena."  This gives Redshift access to the Athena table definitions. 
## Data Warehouse Tables and Views
1. The Redshift Local schema dev_9dt implements the data warehouse tables and views.
2. Table dev_9dt.player_d contains the player attributes needed for the analyis.  It acts as a dimension table.  It does not include PII or protected information.
3. Table dev_9dt.games_f contains the measures at the grain of game_id.  It is a flattened representation of the games.csv data.  Moves data is preserved in games_f.moves as an ordered list.
4. Table dev_9dt.player_metric_games_played contains the number of games played by each player.  It is used for two analysis views, games by nationaility and players who played a single game only.
## Analysis
9. View dev_9dt.games_metric_first_move_percentile_rank_v contains the 1st analysis results for the percentile rank of the first move in a game_id
10. View dev_9dt.games_metric_played_by_nationality_v contains the 2nd analysis results for the games played by nationality
11. View dev_9dt.player_metric_played_single_game_v contains the 3rd analysis results for the players who played only one game, with their win/lose/draw status and email address.
# Usage Notes
1. The Redsfhit analysis views and the local base tables are exposed through the redshift connection details here:
```
redshift_jdbc_endpoint = "redshift-cluster-1.cpykyfqusghx.us-west-1.redshift.amazonaws.com"
redshift_jdbc_port = "5439"
redshift_jdbc_database = "dev9dt"
redshift_jdbc_username = "ninetyeightpoint6"
redshift_jdbc_password = "{shared separately}"
```
2. The redshift user has the following access:
```
grant all on all tables in schema athena_schema to ninetyeightpoint6;
grant all on all tables in schema dev_9dt to ninetyeightpoint6;

grant usage on schema athena_schema to ninetyeightpoint6;
grant usage on schema dev_9dt to ninetyeightpoint6;
```
3. The main.py file writes the users API data to S3.  The S3 bucket can be updated in the variable "bucket_name."
4. The Athena and Redshift setup scripts are in the file setup.py.  These are one-time scripts to create the data wareshoue objects.  Incremental loads are not in scope.
5. Athena is only used as a pass-through to enable Redshift Spectrum access to the players and games data in S3.  Athena access is not included.
6. The IAM users and roles were seutp manually and are not included in this package.
7. The AWS account used to create these objects is a personal account.
