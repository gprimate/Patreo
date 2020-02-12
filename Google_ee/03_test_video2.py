import ee
from ee import batch

## Initialize connection to server
ee.Initialize()

## Define your image collection 
collection = ee.ImageCollection('COPERNICUS/S2')

## Define time range
collection_time = collection.filterDate('2010-01-01', '2020-01-01') #YYYY-MM-DD

## Select location based on location of tile
path = collection_time.filter(ee.Filter.eq('WRS_PATH', 198))
pathrow = path.filter(ee.Filter.eq('WRS_ROW', 24))
# or via geographical location:
#point_geom = ee.Geometry.Point(5, 52) #longitude, latitude
#pathrow = collection_time.filterBounds(point_geom)

## Select imagery with less then 5% of image covered by clouds
clouds = pathrow.filter(ee.Filter.lt('CLOUD_COVER', 5))

## Select bands
bands = clouds.select(['B4', 'B3', 'B2'])

## Make 8 bit data
def convertBit(image):
    return image.multiply(512).uint8()  

## Convert bands to output video  
outputVideo = bands.map(convertBit)

print("Starting to create a video")

x = -43.937127
y = -19.934303
side_size = 0.0086

# Squared centered at x,y
geometry = [[x - side_size, y - side_size], [x - side_size, y + side_size],
            [x + side_size, y + side_size], [x + side_size, y - side_size]]


## Export video to Google Drive
out = batch.Export.video.toDrive(outputVideo, description='Bh_video_region_S2_time', dimensions = 720, framesPerSecond = 2, region=(geometry), maxFrames=100000)

## Process the image
process = batch.Task.start(out)

print("Process sent to cloud")