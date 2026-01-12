calc_max_tailwind <- function(wind_str, rwy_bearing_mag, variation) {
  
  if (is.na(wind_str)){
    return(NA)
  }
  
  #get a list of winds -- e.g. "12010G20KT -> 15015KT" -> [["12010G20", "15015"]]
  wind_list <- str_extract_all(
    wind_str,
    "(VRB\\d{2}|\\d{3}\\d{2}(?:G\\d{2})?)",
    simplify = FALSE
  )[[1]]
  
  if (length(wind_list) == 0) return(NA)
  
  calc_one_tailwind <- function(wind, rwy_bearing, variation) {
    
    # --- VRB case (no gusts possible) ---
    if (str_detect(wind, "^VRB")) {
      speed <- as.numeric(str_extract(wind, "\\d{2}"))
      return(speed)
    }
    
    # --- Fixed-direction wind ---
    wind_dir <- as.numeric(str_extract(wind, "^\\d{3}"))
    wind_speed  <- as.numeric(str_extract(wind, "(?<=^[0-9]{3})[0-9]{2}"))
    
    # If gust present, use gust value
    gust <- str_extract(wind, "(?<=G)\\d{2}")
    if (!is.na(gust)) {
      wind_speed <- as.numeric(gust)
    }
    
    rwy_bearing_true <- rwy_bearing_mag + variation
    
    tailwind <- max(0, -wind_speed * cos(pi / 180 * (rwy_bearing_true - wind_dir)))
    return(tailwind)
  }
  
  #apply to all wind groups and return max tailwind
  tailwinds <- sapply(wind_list, calc_one_tailwind, rwy_bearing_mag, variation)
  tailwinds <- tailwinds[!is.na(tailwinds)]
  
  if (length(tailwinds) == 0) {
    stop(
      paste(
        cat(
          "WARNING: empty tailwinds\n",
          " wind_str:", wind_str, "\n",
          " rwy_bearing_mag:", rwy_bearing_mag, "\n",
          " variation:", variation, "\n",
          " extracted wind_list:", paste(wind_list, collapse = ", "), "\n"
        )
      )
    )
  }
  
  return(max(tailwinds, na.rm = TRUE))
}

round_ceiling_aviation <- function(x) {
  # x in feet AGL (numeric)
  remainder <- x %% 100
  ifelse(remainder <= 20,
         x - remainder,          # round down
         x - remainder + 100)    # round up
}
