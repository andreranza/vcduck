# Analyze ----------------------------------------------------------------------
library(dplyr)
library(ggplot2)
library(DBI)
library(duckdb)
library(dm)

duckdb_con <- dbConnect(
  duckdb(),
  dbdir = "formd.duckdb",
  read_only = FALSE
)

dm_formd <- dm_from_con(duckdb_con, learn_keys = FALSE)

# Filing date analysis ---------------------------------------------------------

filing_date_ym <-
  dm_formd |>
  pull_tbl(form_d) |>
  rename_with(tolower) |>
  mutate(
    date_long = if_else(
      stringr::str_length(filing_date) > 11L,
      filing_date,
      NA_character_
    ),
    date_long = stringr::str_sub(date_long, 1L, 10L),
    date_long = sql("CAST(date_long AS DATE)")
  ) |>
  mutate(
    date_short = if_else(
      stringr::str_length(filing_date) <= 11L,
      filing_date,
      NA_character_
    )
  ) |>
  filter(!is.na(date_short)) |>
  mutate(
    date_short = sql("STRPTIME(date_short, '%d-%b-%Y')")
  ) |>
  mutate(date_short = sql("CAST(date_short AS DATE)")) |>
  transmute(
    accessionnumber,
    filing_date = coalesce(date_long, date_short),
    submissiontype,
    year = lubridate::year(filing_date),
    month = lubridate::month(filing_date)
  ) |>
  count(year, month, submissiontype) |>
  collect() |>
  arrange(year, month)

filing_date_ym |>
  mutate(dte = lubridate::make_date(year, month)) |>
  ggplot(aes(dte, n, color = submissiontype)) +
  geom_line() +
  theme_minimal() +
  theme(
    axis.title.y = element_blank(),
    axis.title.x = element_blank(),
    plot.title = element_text(hjust = 0.5)
  ) +
  labs(title = "Submission across years")
