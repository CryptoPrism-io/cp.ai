library(RMySQL)
library(crypto2)
library(dplyr)

crypto.listings.latest <- crypto_listings(
  which = "latest",
  convert = "USD",
  limit = 5000,
  start_date = Sys.Date()-1,
  end_date = Sys.Date()+1,
  interval = "daily",
  quote = TRUE,
  sort = "cmc_rank",
  sort_dir = "asc",
  sleep = 0,
  wait = 0,
  finalWait = FALSE
)

# Filter the data based on cmc_rank
crypto.listings.latest<- crypto.listings.latest %>%
  filter(cmc_rank > 0 & cmc_rank < 250)

all_coins<-crypto_history(coin_list = crypto.listings.latest,convert = "USD",limit = 200,
                          start_date = Sys.Date()-5,end_date = Sys.Date()+1,sleep = 0,interval="hourly")

all_coins <- all_coins[, c("id", "slug", "name", "symbol", "timestamp", "open","high", "low", "close", "volume", "market_cap")]

# Load necessary libraries
library(DBI)
library(RPostgres)

# Connection parameters
db_host <- "34.55.195.199"         # Public IP of your PostgreSQL instance on GCP
db_name <- "cp_ai"                  # Database name
db_user <- "yogass09"              # Database username
db_password <- "jaimaakamakhya"    # Database password
db_port <- 5432                    # PostgreSQL port

# Attempt to establish a connection
con <- dbConnect(
  RPostgres::Postgres(),
  host = db_host,
  dbname = db_name,
  user = db_user,
  password = db_password,
  port = db_port
)

# Check if the connection is valid
if (dbIsValid(con)) {
  print("Connection successful")
} else {
  print("Connection failed")
}

# Write dataframes to database
dbWriteTable(con, "ohlcv_1h_250_coins", all_coins, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "crypto_listings_latest", crypto.listings.latest, overwrite = TRUE, row.names = FALSE)
# Close connection
dbDisconnect(con)
