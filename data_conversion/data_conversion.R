library(readr)
files <- list.files(file.path("..", "processed"))
for (f in files) {
  tmp <- readr::read_csv(file.path("..", "processed", files[7]))
  filename <- gsub(".csv", "", f)
  saveRDS(tmp, file = file.path("..", "processed", paste0(filename, ".rds")))  
}