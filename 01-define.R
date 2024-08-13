library(DBI)
library(duckdb)
library(dplyr)

# need to download manually
try(
  download.file(
    "https://www.sec.gov/files/structureddata/data/form-d-data-sets/2024q2_d.zip",
    destfile = tempfile(fileext = ".zip")
  )
)

sec_paths <- fs::dir_ls("formd")

if (!length(fs::dir_ls("formd-unzipped")) > 0) {
  purrr::walk(sec_paths, ~ unzip(.x, exdir = "formd-unzipped"))
}

fs::dir_ls("formd-unzipped", recurse = TRUE)

if (fs::file_exists("formd.duckdb")) {
  fs::file_delete("formd.duckdb")
}

duckdb_con <- dbConnect(
  duckdb(),
  dbdir = "formd.duckdb",
  read_only = FALSE
)

# Data model -------------------------------------------------------------------

# https://duckdb.org/docs/guides/performance/schema.html#constraints

if (!dbExistsTable(duckdb_con, "form_d")) {
  dbCreateTable(
    duckdb_con,
    "form_d",
    fields = c(
      ACCESSIONNUMBER = "varchar(20) primary key not null",
      FILING_DATE = "varchar(8)",
      SIC_CODE = "varchar(4)",
      SUBMISSIONTYPE = "varchar(255) not null",
      OVER100PERSONSFLAG = "varchar(5)",
      OVER100ISSUERFLAG = "varchar(5)"
    )
  )
}

# verify PK
dbGetQuery(duckdb_con, "PRAGMA table_info('form_d');") |>
  filter(pk) |>
  count()

# composite primary key
if (!dbExistsTable(duckdb_con, "issuers")) {
  cr_q_issuers <-
     "CREATE OR REPLACE TABLE issuers (
      ACCESSIONNUMBER varchar(20) not null,
      IS_PRIMARYISSUER_FLAG varchar(3) not null,
      ISSUER_SEQ_KEY numeric(38) not null,
      primary key (ACCESSIONNUMBER, ISSUER_SEQ_KEY),
      foreign key (ACCESSIONNUMBER) references form_d(ACCESSIONNUMBER),
      ENTITYNAME varchar(150),
      CITY varchar(30) not null,
      STATEORCOUNTRY varchar(255) not null,
      STATEORCOUNTRYDESCRIPTION varchar(50),
      JURISDICTIONOFINC varchar(50),
      ENTITYTYPE varchar(255) not null,
      ENTITYTYPEOTHERDESC varchar(255),
      YEAROFINC_TIMESPAN_CHOICE varchar(150) not null,
      YEAROFINC_VALUE_ENTERED varchar(4)
    );"

  dbExecute(duckdb_con, cr_q_issuers)
}

dbGetQuery(duckdb_con, "PRAGMA table_info('issuers');")

if (!dbExistsTable(duckdb_con, "offering")) {
  # TODO: this is too much I should choose just a few
  cr_q_offering <-
     "CREATE OR REPLACE TABLE offering (
      ACCESSIONNUMBER varchar(20) not null primary key,
      foreign key (ACCESSIONNUMBER) references form_d(ACCESSIONNUMBER),
      INDUSTRYGROUPTYPE varchar(255) not null,
      INVESTMENTFUNDTYPE varchar(255),
      IS40ACT varchar(5),
      REVENUERANGE varchar(255),
      AGGREGATENETASSETVALUERANGE varchar(255),
      FEDERALEXEMPTIONS_ITEMS_LIST varchar(1000),
      ISAMENDMENT varchar(5) not null,
      PREVIOUSACCESSIONNUMBER varchar(20),
      SALE_DATE varchar(255),
      YETTOOCCUR varchar(5),
      MORETHANONEYEAR varchar(5) not null,
      ISEQUITYTYPE varchar(5),
      ISDEBTTYPE varchar(5),
      ISOPTIONTOACQUIRETYPE varchar(5),
      ISSECURITYTOBEACQUIREDTYPE varchar(5),
      ISPOOLEDINVESTMENTFUNDTYPE varchar(5),
      ISTENANTINCOMMONTYPE varchar(5),
      ISMINERALPROPERTYTYPE varchar(5),
      ISOTHERTYPE varchar(5),
      DESCRIPTIONOFOTHERTYPE varchar(255),
      ISBUSINESSCOMBINATIONTRANS varchar(5) not null,
      BUSCOMBCLARIFICATIONOFRESP varchar(255),
      MINIMUMINVESTMENTACCEPTED numeric(19) not null,
      OVER100RECIPIENTFLAG varchar(5),
      TOTALOFFERINGAMOUNT varchar(12) not null,
      TOTALAMOUNTSOLD numeric(12) not null,
      TOTALREMAINING varchar(12) not null,
      SALESAMTCLARIFICATIONOFRESP varchar(255),
      HASNONACCREDITEDINVESTORS varchar(5) not null,
      NUMBERNONACCREDITEDINVESTORS numeric(19),
      TOTALNUMBERALREADYINVESTED numeric(19) not null,
      SALESCOMM_DOLLARAMOUNT numeric(12) not null,
      SALESCOMM_ISESTIMATE varchar(5),
      FINDERSFEE_DOLLARAMOUNT numeric(12) not null,
      FINDERSFEE_ISESTIMATE varchar(5),
      FINDERFEECLARIFICATIONOFRESP varchar(255),
      GROSSPROCEEDSUSED_DOLLARAMOUNT numeric(12) not null,
      GROSSPROCEEDSUSED_ISESTIMATE varchar(5),
      GROSSPROCEEDSUSED_CLAROFRESP varchar(255),
      AUTHORIZEDREPRESENTATIVE varchar(5)
    )"

  dbExecute(duckdb_con, cr_q_offering)
}

if (!dbExistsTable(duckdb_con, "recipients")) {
  cr_q_recipients <-
     "CREATE TABLE recipients (
      ACCESSIONNUMBER varchar(20) not null,
      RECIPIENT_SEQ_KEY numeric(38) not null,
      primary key (ACCESSIONNUMBER, RECIPIENT_SEQ_KEY),
      foreign key (ACCESSIONNUMBER) references form_d(ACCESSIONNUMBER),
      RECIPIENTNAME varchar(150) not null,
      RECIPIENTCRDNUMBER varchar(9) not null,
      ASSOCIATEDBDNAME varchar(150) not null,
      ASSOCIATEDBDCRDNUMBER varchar(9) not null,
      STREET1 varchar(40) not null,
      STREET2 varchar(40),
      CITY varchar(30) not null,
      STATEORCOUNTRY varchar(255) not null,
      STATEORCOUNTRYDESCRIPTION varchar(50),
      ZIPCODE varchar(10) not null,
      STATES_OR_VALUE_LIST varchar(1000),
      DESCRIPTIONS_LIST varchar(1000),
      FOREIGNSOLICITATION varchar(5)
    )"

  dbExecute(duckdb_con, cr_q_recipients)
}

dbGetQuery(duckdb_con, "PRAGMA table_info('issuers');")

dbDisconnect(duckdb_con)
