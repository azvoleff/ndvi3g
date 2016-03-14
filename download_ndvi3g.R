library(raster)
library(itertools)
library(foreach)
library(doParallel)

registerDoParallel(3)

Sys.setenv(AWS_CONFIG_FILE='C:/Users/azvol/.aws/config')

# Function to generate a unique temporary directory name separate from the R 
# session temp folder.
get_tempdir <- function() {
    rand_str <- function() {
        paste(sample(c(0:9, letters, LETTERS), 10, replace=TRUE), collapse='')
    }
    rand_dir <- paste0(tempdir(), '_', rand_str())
    while(!dir.create(rand_dir, showWarnings=FALSE)) {
        rand_dir <- paste0(tempdir(), '_', rand_str())
    }
    return(rand_dir)
}

source('s3_ls.R')

files <- s3_ls('nasanex/AVHRR/GIMMS/3G/', recursive=TRUE)
files$s3_url <- paste0('s3://nasanex/', files$file)
#files$url <- paste0('http://nasanex.s3.amazonaws.com/', files$file)
files <- files[!grepl('.txt$', files$file), ]

# TODO: Calculate dates from filenames

files <- files[1:4, ]

ndvi <- foreach(ndvi_file=iter(files, by="row"), .combine=c,
                .packages=c('raster')) %dopar% {
    temp_dir <- get_tempdir()
    system2('aws', args=c('s3', 'cp', ndvi_file$s3_url, temp_dir), stdout=NULL)
    d <- file(file.path(temp_dir, basename(ndvi_file$file)), "rb")
    ndvi_values <- readBin(d, integer(), size=2, n=4320*2160, endian='big')
    # Mask out water and nodata
    close(d)
    unlink(temp_dir, recursive=TRUE)
    r <- raster(matrix(ndvi_values, ncol=4320, nrow=2160))
    r[r == -10000] <- NA # missing data
    r[r == -5000] <- NA # water mask
    # Round to range from -255 - 255 for size.
    # Note initial data was scaled by 10000.
    r <- round((r / 10000) * 127)
}

# TODO: Fix this to combine list of spatial polygons
writeRaster(ndvi, filename='ndvi.tif', datatype='INT1S')

ndvi_spdf <- rasterToPolygons(ndvi, fun=function(x) {x > -20}, dissolve=TRUE)
