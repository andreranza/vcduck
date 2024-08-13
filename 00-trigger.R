scripts_path <- fs::dir_ls(regexp = "^0[1-9]-\\w+\\.R$")

purrr::walk(
  scripts_path,
  function(x) {
    message("Running ", basename(x))
    source(x, echo = TRUE, encoding = "UTF-8", local = TRUE)
  }
)
