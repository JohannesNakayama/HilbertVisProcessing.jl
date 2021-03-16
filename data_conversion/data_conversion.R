library(readr)
library(magrittr)
library(dplyr)
library(stringr)
files <- list.files(file.path("..", "processed"))
for (f in files) {
  tmp <- readr::read_csv(file.path("..", "processed", f))
  filename <- gsub(".csv", "", f)
  
  tmp %<>%
    mutate(search_date = lubridate::as_date(search_date)) %>% # Fix date as only days
    mutate(url = str_replace_all(sourceUrl, "^[.](.+)", "\\1")) %>%
    mutate(url = str_remove(url, pattern = "&sa=.*$")) # remove left over google analytics url fragments
  
  tmp %<>% 
    filter(rank <= 20) %>%
    filter(language == "de") %>% 
    filter(url != "google") %>% 
    filter(url != "no source") %>% 
    filter(domain != "no source") %>% 
    group_by(keyword, search_date, url, domain) %>%
    summarise(rank = sum(1/(rank + 1))) %>%
    ungroup()
  
  saveRDS(tmp, file = file.path("..", "processed", paste0(filename, ".rds")))  
  rm(tmp)
  gc(full = TRUE)
}
