import ee
from ee import batch

## Initialize connection to server
ee.Initialize()

## Define your image collection 
collection = ee.ImageCollection('LANDSAT/LC8_L1T_TOA')

## Define time range
collection_time = collection.filterDate('2013-04-11', '2020-01-01') #YYYY-MM-DD

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

## Export video to Google Drive
out = batch.Export.video.toDrive(outputVideo, description='Netherlands_video_region_L8_time_3', dimensions = 720, framesPerSecond = 2, region=([5.588144,51.993435], [5.727906, 51.993435],[5.727906, 51.944356],[5.588144, 51.944356]), maxFrames=10000)

## Process the image
process = batch.Task.start(out)

print("Process sent to cloud")