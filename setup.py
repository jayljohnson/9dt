from setuptools import setup

setup(name='9dt',
      version='0.1',
      description='players, games',
      url='http://github.com/jayljohnson/9dt',  ## 'http://github.com/.../...'
      author='Jay L. Johnson',
      author_email='jljohn00@gmail.com',
      license='...',
      packages=['...'],
      zip_safe=False)
      
#Database Scripts
copy_games_date_to_s3 = 
"""
# This was done manually using pdf link https://s3-us-west-2.amazonaws.com/98point6homework-assets/game_data.csv. Saved to s3://9dt-jayljohnson/games
# Unable to read the data directly from Athena DDL.  
# TODO: see if the data can be read directly. 
"""

athena_create_schema = """
create database dev9dt_athena;
"""

athena_create_table_players = """
CREATE EXTERNAL TABLE dev9dt_athena.players (
  id int,
  data struct<gender:string, name:string, `location`:string, email:string, login:string, dob:string, registered:string, phone:string, cell:string, id:string, picture:string, nat:string>
  )
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://9dt-jayljohnson/players/';
"""
/
athena_create_table_games = """
CREATE EXTERNAL TABLE dev9dt_athena.games (
  game_id string,
  player_id int,
  move_number int,
  `column` int,
  result string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
LINES TERMINATED BY '\n'
LOCATION 's3://9dt-jayljohnson/games'
TBLPROPERTIES ("skip.header.line.count"="1");
"""

redshift_create_external_schema = """
create external schema athena_schema if not exists from data catalog 
database 'dev9dt_athena' 
iam_role 'arn:aws:iam::967095003863:role/dev-9dt-redshift'
region 'us-west-1'
"""

redshift_setup_and_load_initial_tables = """
create schema if not exists dev_9dt;

/*
 * Limiting the data to only the attributes needed for analysis, currently 'nat' and 'email' 
 * Need to understand PII requirements in the data warehouse before including other attributes.
 * Assumption: athena_schema.players and the S3 data store are secured to PII standards
 */
create table dev_9dt.player_d as (
	select 
	  id as player_id
	, data.nat as nationality
	, data.email as email
	from athena_schema.players
);

/*
 * Fact table with game metrics
 * Assumption: Game data does not change.  If new metrics are needed in the future, decide on backfill requirements at that time.
 */
create table dev_9dt.games_f as (
	with 
	/*
	 * Get the player id's for players 1 & 2 on a single row for each game_id
	 * Get the column played by player 1 in their first move
     *
	 * 	 Also tried using NTH_VALUE approach below but the execution plan was much better with rownum based approach.  Keeping here for reference.
	 * Rownum approach is more verbose, but seems easier to understand and maintain than the ROWS clause in the window function.
	 * 	
	    select 
		  distinct game_id
		, NTH_VALUE(player_id, 1) over (partition by game_id order by move_number ROWS BETWEEN UNBOUNDED PRECEDING AND 0 FOLLOWING) as player_1
		, NTH_VALUE(player_id, 2) over (partition by game_id order by move_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) as player_2
		, NTH_VALUE("column", 1) over (partition by game_id order by move_number ROWS BETWEEN UNBOUNDED PRECEDING AND 0 FOLLOWING) as first_move_column
		from athena_schema.games
	 */
	game_players as (
		with 
		/*
		 * Get the first and second player from the first two moves of each game. 
		 */
		game_players_ordered as (
			select *
			from (
				select 
				  game_id
				, player_id
				, row_number() over (partition by game_id order by game_id, move_number) rownum
				from athena_schema.games)
			where rownum <= 2
		)
		select 
		  gp1.game_id
		, gp1.player_id as player_1
		, gp2.player_id as player_2
		from game_players_ordered gp1
		join game_players_ordered gp2 on gp1.game_id = gp2.game_id
		where gp1.rownum = 1
		and gp2.rownum = 2
	),
	/*
	 * Aggregate the moves values into an ordered list for each game_id to preserve the moves data for analysis
	 * Get the result value of either win or draw for the game using max().  Because other values are null except for the last move, the final game status is returned.
	 */
	game_moves as (
		select 
		  gp.game_id
		, gp.player_1 
		, gp.player_2
		, listagg(g."column", ',') within group (order by move_number) as moves
		, max("result") as game_result
		from athena_schema.games g
		join game_players gp on g.game_id = gp.game_id 
		group by 
		  gp.game_id
		, gp.player_1 
		, gp.player_2 
	),
	/*
	 * Get the player who made the last move in the game
	 * If the result is 'win' the same player won the game and the other player lost the game.
	 * The mod 2 (% 2) function on the moves list is faster than another window function scan to get the last move for each game. 
	 * If the number of moves in a game is an even number, player 2 had the last move.  Else player 1 had the last move.  
	 *   Uses mod 2 where remainder of 0 is an even number, 1 is an odd number
	 */
	game_move_metrics as (
		select 
		  gm.game_id
		, regexp_count(gm.moves, ',') + 1 as number_of_moves_in_game
		, SPLIT_PART(gm.moves, ',',1) as first_move_column
		, case (regexp_count(gm.moves, ',') + 1) % 2 
		       when 1 
		       then 1 
		       else 2 
	      end as last_move_in_game_by_player
		from game_moves gm
	)
	select 
	  gm.game_id
	, gm.player_1
	, gm.player_2
	, gm.moves
	, gmm.first_move_column
	, gm.game_result
	, gmm.number_of_moves_in_game
	, case when gm.game_result = 'draw' 
		   then -1 --If game is a draw, there is no player_id that won.  Default to dummy value -1.
	       when gm.game_result = 'win' and gmm.last_move_in_game_by_player = 1 
	       then player_1 
	  	   else player_2 
  	  end as player_id_win
	, case when gm.game_result = 'draw' 
	       then -1 --If game is a draw, there is no player_id that lost.  Default to dummy value -1.
	       when gm.game_result = 'win' and gmm.last_move_in_game_by_player = 1 
	       then player_2 
	  	   else player_1 
	  end as player_id_lose
	, case when gm.game_result = 'draw' and gmm.number_of_moves_in_game = 16 
	       then true 
	       else false 
	  end as game_is_draw --Only games that filled the board with 16 moves should be draws.  Including extra validation for this.	TODO: Edge case for abandoned games?
	from game_moves gm
	join game_move_metrics gmm on gm.game_id = gmm.game_id
);
		
/* 
 * percentile rank metrics
*/
create or replace view dev_9dt.games_metric_first_move_percentile_rank_v as (
	with win_count as (
		select 
		  first_move_column 
		, count(1) as win_count
		from dev_9dt.games_f
		where player_id_win = player_1
		group by 1
	)
	select 
	  first_move_column
	, win_count 
	, percent_rank()
	  over (order by win_count)
	from win_count
);

/*
 * Games by nationality metrics
 * TODO: If two people of the same nationality play in the same game, count once or twice?
 * Validation: Since each game has 2 players, the number of games played by person or dimension of person is 2x the number of games in f_games
 */
create table dev_9dt.player_metric_games_played as (
	/*
	 * Get the list of players per game from the table games_f. 
	 * Driving this from the games_f table instead of the base table for data consistency. 
	 *   For example, reads from the base table while games_f is being loaded could give inconsistent results.
	 */
	with unique_players as (
		select 
		  player_1 as player_id
	    , game_id
		from dev_9dt.games_f g 
		union
		  select player_2 as player_id
		, game_id
		from dev_9dt.games_f g
	)
	/*
	 * Get the count of games per player
	 */
	select 
	  player_id
	, count(game_id) games_played
	from unique_players
	group by 1);
	
create or replace view dev_9dt.games_metric_played_by_nationality_v as (	
	select 
	  p.nationality
	, sum(pgp.games_played) as games_played
	from dev_9dt.player_d p 
	join dev_9dt.player_metric_games_played pgp on p.player_id = pgp.player_id
	group by 1);

/*
 * Email campaign metrics for single game players
 */
create or replace view dev_9dt.player_metric_played_single_game_v as (	
	select 
	  pgp.player_id
	, case when g.game_is_draw 
	       then 'drew' 
	       when pgp.player_id = g.player_id_win 
	       then 'won'
	       when pgp.player_id = g.player_id_lose 
	       then 'lost'
	  	   else null
	  end as game_status
	, p.email
	, p.nationality
	, pgp.games_played 
	from dev_9dt.player_metric_games_played pgp
	join dev_9dt.games_f g on (pgp.player_id = g.player_1 or pgp.player_id = g.player_2) 
	join dev_9dt.player_d p on pgp.player_id = p.player_id 
	where pgp.games_played = 1
);
"""