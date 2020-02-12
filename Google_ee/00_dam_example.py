# Import the Earth Engine Python Package
import ee
import pandas as pd
import numpy as np
import time

import os

data = pd.read_csv('barragem_2019_novas_coordenadas.tsv', sep= '\t', usecols=[3, 4])

data = data.values

# Para mudar de conta:
# earthengine authenticate

# Initialize the Earth Engine object, using the authentication credentials.
ee.Initialize()

def degree_conv(var):
    data = var.split("°",1)
    if data[0][0] == '-':
        d = data[0][1:]
        negative = True
    else:
        d = data[0]
        negative = False
    data = data[1].split("'",1)
    minutes = data[0]
    data = data[1].split('"',2)
    sec =  data[0]

    #DMS to decimal degrees converter
    if negative:
        dd = (int(d) + (float(minutes)/60) + (float(sec)/3600)) * -1
    else:
        dd = int(d) + (float(minutes)/60) + (float(sec)/3600)
    return round(dd, 6)
 

# satellites = ["sentinel", "landsat8"]
satellites = ["sentinel"]

dates = [['2018-01-01','2018-12-31'], ['2017-01-01','2017-12-31'], ['2016-01-01','2016-12-31']]
#dates = [['2019-01-01','2020-01-01']]

not_dam = 1

satellites = ["sentinel"]

north_ = [True, False]

for north in north_:
    for satellite_name in satellites:
        for day in dates:
            for dam in range(data.shape[0]):
                ee.Initialize()

                if satellite_name == "sentinel":

                    # Use these bands for prediction.
                    bands = ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8','B8A', 'B9', 'B10', 'B11', 'B12']

                    # Use Sentinel 2 surface reflectance data.
                    satellite = ee.ImageCollection("COPERNICUS/S2")

                    flag_clouds = 'CLOUDY_PIXEL_PERCENTAGE'

                    def maskS2clouds(image):
                        cloudShadowBitMask = ee.Number(2).pow(3).int()
                        cloudsBitMask = ee.Number(2).pow(5).int()
                        qa = image.select('QA60')
                        mask = qa.bitwiseAnd(cloudShadowBitMask).eq(0).And(
                            qa.bitwiseAnd(cloudsBitMask).eq(0))
                        return image.updateMask(mask).select(bands).divide(10000)

                    mask = maskS2clouds

                if satellite_name == "landsat8":

                    # Use these bands for prediction.
                    bands = ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B9', 'B10', 'B11']

                    # Use Sentinel 2 surface reflectance data.
                    satellite = ee.ImageCollection("LANDSAT/LC08/C01/T1_TOA")

                    flag_clouds = 'CLOUD_COVER'

                    # Cloud masking function.
                    def maskL8sr(image):
                        cloudsBitMask = ee.Number(2).pow(4).int()
                        qa = image.select('BQA')
                        mask = qa.bitwiseAnd(cloudsBitMask).eq(0)
                        return image.updateMask(mask).select(bands).divide(10000)

                    mask = maskL8sr

                # Logitude, Latitude 
                x, y = degree_conv(data[dam][1]), degree_conv(data[dam][0])
                #x, y = float(data[dam][1].replace(",", ".")), float(data[dam][0].replace(",", "."))

                if not_dam:
                    if north:
                        y = round(y + 0.0172*2, 6)
                        string = "N"
                    else:
                        y = round(y - 0.0172*2, 6)
                        string = "S"

                llx = x - 0.0172
                lly = y - 0.0172
                urx = x + 0.0172
                ury = y + 0.0172

                geometry = [[llx,lly], [llx,ury], [urx,ury], [urx,lly]]

                cloudy_percentage = 30

                # The image input data is cloud-masked median composite.
                dataset = satellite.filterDate(day[0],day[1]).filter(ee.Filter.lte(flag_clouds, cloudy_percentage)).map(mask).filterBounds(ee.Geometry.Polygon(geometry))

                try:
                    while ((dataset.size().getInfo() == 0) and (cloudy_percentage <= 100)):
                        print(dataset.size().getInfo())
                        print(cloudy_percentage)
                        cloudy_percentage = cloudy_percentage + 10
                        dataset = satellite.filterDate(day[0],day[1]).filter(ee.Filter.lte(flag_clouds, cloudy_percentage)).map(mask).filterBounds(ee.Geometry.Polygon(geometry))

                    image = dataset.median()

                    if not_dam:
                        check_intersection = False
                        for check in range(data.shape[0]):
                            if check != dam:
                                x_, y_ = degree_conv(data[check][1]), degree_conv(data[check][0])
                                if (llx < x_ < urx) and (lly < y_ < ury):
                                    check_intersection = True

                        if not check_intersection:
                            task = ee.batch.Export.image.toDrive(image=image.toFloat(),
                                             description="{0}{1:0=3d}_{2}".format(string, dam+1, day[0][:4]),
                                             folder=day[0][:4]+"_NOT_DAM",
                                             region=geometry,
                                             scale=10,
                                             shardSize=384,
                                             fileDimensions=(384, 384),
                                             fileFormat='GeoTIFF')
                            print("{0}{1:0=3d}_{2}".format(string, dam+1, day[0][:4]))
                            task.start()
                    else:
                        task = ee.batch.Export.image.toDrive(image=image.toFloat(),
                                                         description="{0:0=5d}_{1}_other".format(dam+1, day[0][:4]),
                                                         folder="others_dam",
                                                         region=geometry,
                                                         scale=10,
                                                         shardSize=384,
                                                         fileDimensions=(384, 384),
                                                         fileFormat='GeoTIFF')
                        print("{0:0=5d}_{1}_other".format(dam+1, day[0][:4]))
                        
                        
                        task.start()
                except:
                    try:
                        print("using exception")
                        time.sleep(60)

                        while ((dataset.size().getInfo() == 0) and (cloudy_percentage <= 100)):
                            print(dataset.size().getInfo())
                            print(cloudy_percentage)
                            cloudy_percentage = cloudy_percentage + 10
                            dataset = satellite.filterDate(day[0],day[1]).filter(ee.Filter.lte(flag_clouds, cloudy_percentage)).map(mask).filterBounds(ee.Geometry.Polygon(geometry))

                        image = dataset.median()

                        if not_dam:
                            check_intersection = False
                            for check in range(data.shape[0]):
                                if check != dam:
                                    x_, y_ = degree_conv(data[check][1]), degree_conv(data[check][0])
                                    if (llx < x_ < urx) and (lly < y_ < ury):
                                        check_intersection = True

                            if not check_intersection:
                                task = ee.batch.Export.image.toDrive(image=image.toFloat(),
                                                 description="{0}{1:0=3d}_{2}".format(string, dam+1, day[0][:4]),
                                                 folder=day[0][:4]+"_NOT_DAM",
                                                 region=geometry,
                                                 scale=10,
                                                 shardSize=384,
                                                 fileDimensions=(384, 384),
                                                 fileFormat='GeoTIFF')
                                print("{0}{1:0=3d}_{2}".format(string, dam+1, day[0][:4]))
                                task.start()
                        else:
                            task = ee.batch.Export.image.toDrive(image=image.toFloat(),
                                                             description="{0:0=5d}_{1}_other".format(dam+1, day[0][:4]),
                                                             folder="others_dam",
                                                             region=geometry,
                                                             scale=10,
                                                             shardSize=384,
                                                             fileDimensions=(384, 384),
                                                             fileFormat='GeoTIFF')
                            print("{0:0=5d}_{1}_other".format(dam+1, day[0][:4]))
                            
                            
                            task.start()
                    except:
                        print("using exception_2")
                        time.sleep(60)

                        while ((dataset.size().getInfo() == 0) and (cloudy_percentage <= 100)):
                            print(dataset.size().getInfo())
                            print(cloudy_percentage)
                            cloudy_percentage = cloudy_percentage + 10
                            dataset = satellite.filterDate(day[0],day[1]).filter(ee.Filter.lte(flag_clouds, cloudy_percentage)).map(mask).filterBounds(ee.Geometry.Polygon(geometry))

                        image = dataset.median()

                        if not_dam:
                            check_intersection = False
                            for check in range(data.shape[0]):
                                if check != dam:
                                    x_, y_ = degree_conv(data[check][1]), degree_conv(data[check][0])
                                    if (llx < x_ < urx) and (lly < y_ < ury):
                                        check_intersection = True

                            if not check_intersection:
                                task = ee.batch.Export.image.toDrive(image=image.toFloat(),
                                                 description="{0}{1:0=3d}_{2}".format(string, dam+1, day[0][:4]),
                                                 folder=day[0][:4]+"_NOT_DAM",
                                                 region=geometry,
                                                 scale=10,
                                                 shardSize=384,
                                                 fileDimensions=(384, 384),
                                                 fileFormat='GeoTIFF')
                                print("{0}{1:0=3d}_{2}".format(string, dam+1, day[0][:4]))
                                task.start()
                        else:
                            task = ee.batch.Export.image.toDrive(image=image.toFloat(),
                                                             description="{0:0=5d}_{1}_other".format(dam+1, day[0][:4]),
                                                             folder="others_dam",
                                                             region=geometry,
                                                             scale=10,
                                                             shardSize=384,
                                                             fileDimensions=(384, 384),
                                                             fileFormat='GeoTIFF')
                            print("{0:0=5d}_{1}_other".format(dam+1, day[0][:4]))
                            
                            
                            task.start()
                        

                    # if((dam+1)%1000 == 0):
                    #     time.sleep(60*60)



    # for satellite_name in satellites:
#     for day in dates:
#         for dam in ids: #range(data.shape[0]):

#             if satellite_name == "sentinel":

#                 # Use these bands for prediction.
#                 bands = ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8','B8A', 'B9', 'B10', 'B11', 'B12']

#                 # Use Sentinel 2 surface reflectance data.
#                 satellite = ee.ImageCollection("COPERNICUS/S2")

#                 flag_clouds = 'CLOUDY_PIXEL_PERCENTAGE'

#                 def maskS2clouds(image):
#                     cloudShadowBitMask = ee.Number(2).pow(3).int()
#                     cloudsBitMask = ee.Number(2).pow(5).int()
#                     qa = image.select('QA60')
#                     mask = qa.bitwiseAnd(cloudShadowBitMask).eq(0).And(
#                         qa.bitwiseAnd(cloudsBitMask).eq(0))
#                     return image.updateMask(mask).select(bands).divide(10000)

#                 mask = maskS2clouds

#             if satellite_name == "landsat8":

#                 # Use these bands for prediction.
#                 bands = ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B9', 'B10', 'B11']

#                 # Use Sentinel 2 surface reflectance data.
#                 satellite = ee.ImageCollection("LANDSAT/LC08/C01/T1_TOA")

#                 flag_clouds = 'CLOUD_COVER'

#                 # Cloud masking function.
#                 def maskL8sr(image):
#                     cloudsBitMask = ee.Number(2).pow(4).int()
#                     qa = image.select('BQA')
#                     mask = qa.bitwiseAnd(cloudsBitMask).eq(0)
#                     return image.updateMask(mask).select(bands).divide(10000)

#                 mask = maskL8sr

#             # Logitude, Latitude 
#             x, y = degree_conv(data[dam][1]), degree_conv(data[dam][0])

#             if not_dam:
#                 if north:
#                     y = round(y + 0.0172*2, 6)
#                     string = "N"
#                 else:
#                     y = round(y - 0.0172*2, 6)
#                     string = "S"

#             llx = x - 0.0172 #0.02785  #0.1114 
#             lly = y - 0.0172 #0.021395 #0.08558 
#             urx = x + 0.0172 #0.02785  #0.1114 
#             ury = y + 0.0172 #0.021395 #0.08558 

#             geometry = [[llx,lly], [llx,ury], [urx,ury], [urx,lly]]

#             cloudy_percentage = 20

#             # The image input data is cloud-masked median composite.
#             dataset = satellite.filterDate(day[0],day[1]).filterBounds(ee.Geometry.Polygon(geometry)).filter(ee.Filter.lte(flag_clouds, cloudy_percentage))

#             while ((dataset.size().getInfo() == 0) and (cloudy_percentage <= 100)):
#                 print(dataset.size().getInfo())
#                 print(cloudy_percentage)
#                 cloudy_percentage = cloudy_percentage + 10
#                 dataset = satellite.filterDate(day[0],day[1]).filterBounds(ee.Geometry.Polygon(geometry)).filter(ee.Filter.lte(flag_clouds, cloudy_percentage))

#             image = dataset.median()

#             if not_dam:
#                 check_intersection = False
#                 for check in range(data.shape[0]):
#                     if check != dam:
#                         x_, y_ = degree_conv(data[check][2]), degree_conv(data[check][1])
#                         if (llx < x_ < urx) and (lly < y_ < ury):
#                             check_intersection = True

#                 if not check_intersection:
#                     task = ee.batch.Export.image.toDrive(image=image.toFloat(),
#                                      description="{0}{1:0=3d}_{2}".format(string, dam+1, day[0][:4]),
#                                      folder="fixed",
#                                      region=geometry,
#                                      scale=10,
#                                      shardSize=384,
#                                      fileDimensions=(384, 384),
#                                      fileFormat='GeoTIFF')
#                     print("{0}{1:0=3d}_{2}".format(string, dam+1, day[0][:4]))
#                     task.start()
#             else:
#                 task = ee.batch.Export.image.toDrive(image=image.toFloat(),
#                                                  description="{0:0=3d}_{1}".format(dam+1, day[0][:4]),
#                                                  folder="fixed"+"_"+satellite_name+"_2019",
#                                                  region=geometry,
#                                                  scale=10,
#                                                  shardSize=384,
#                                                  fileDimensions=(384, 384),
#                                                  fileFormat='GeoTIFF')
#                 print("{0:0=3d}_{1}".format(dam+1, day[0][:4]))
#                 task.start()
#     # time.sleep(30*60)    
