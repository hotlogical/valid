import pandas as pd
import math
import matplotlib.path as mpltPath
import numpy as np

uniqueid = 100000000000
csv_header = 'store_id,Store Name,address1,address2,city,state,zip_code (postal code),Country alpha-3,Longitude,Latitude,Polygon(WKT),tradingArea(miles),brandID,brandName'
csv_rows = [csv_header]

def get_polygon(polygon_str):
        polygon_points = polygon_str.replace('POLYGON ((', '').replace('))', '').split(', ')
        return np.array([[float(point) for point in p.split()] for p in polygon_points])

def get_extremes(polygon):
    dfll = pd.DataFrame(polygon, columns='lon lat'.split())
    return [dfll.lat.min(), dfll.lat.max(), dfll.lon.min(), dfll.lon.max()]

def get_grid(extremes, degree_increment):
    latinc = degree_increment
    loninc = degree_increment
    latmin, latmax, lonmin, lonmax = extremes
    latave = 0.5 * (latmin + latmax)
    lat = latmax + latinc
    lon = lonmin - loninc
    equr = 6378.1370
    polr = 6356.7523
    latr = equr - ((latave / 90.) * (equr - polr))  # radius at latitude r
    lat1km = (math.pi / 180.) * latr  # 1 degree of lat in km
    radius = latinc * lat1km / math.sqrt(2.) / 1.6  # 1 degree = 111km,
    i = 0
    points = []
    thisloninc = loninc
    while lat > latmin - latinc:
        j = 0
        thisloninc = loninc / math.cos((lat / 180.) * math.pi)
        while lon < lonmax + thisloninc:
            points.append([lon, lat])
            lon += thisloninc
            j += 1
        lon = lonmin
        lat -= latinc
        i += 1
    # dfpnts = pd.DataFrame(points, columns='lon lat'.split())
    print(len(points), i, j, degree_increment, radius)
    return np.array(points), radius, latinc, thisloninc

def truncate_grid(polygon, grid):
    path = mpltPath.Path(polygon)
    good_point_mask = path.contains_points(grid)
    return grid[good_point_mask]

def make_csv_rows(row, points, radius):
    global uniqueid
    rows = [f'{uniqueid + i},grid_{row.name}_{i},{row.name},,,,,{row.country3},{p[0]},{p[1]},,{radius:.10f},{row.Index + 10},{row.name}'
            for i, p in enumerate(points)]
    uniqueid += len(rows)
    # print(row.name, '\t', len(rows), row.degree_increment, radius)
    return rows

def make_poi_grids(csv_file):
    dfp = pd.read_csv(csv_file)
    info_rows = ['Area,Id,Country,num_rows,radius,degree_inc,latinc,loninc,latmin,latmax,lonmin,lonmax']
    for row in dfp.itertuples():
        print(row.name)
        polygon = get_polygon(row.WKT)
        extremes = get_extremes(polygon)
        grid, radius , latinc, loninc= get_grid(extremes, row.degree_increment)
        points = truncate_grid(polygon, grid)
        new_csv_rows = make_csv_rows(row, points, radius)
        print(len(new_csv_rows), extremes, 0.5*(sum(extremes[:2])), 0.5*(sum(extremes[2:])), extremes[1] - extremes[0])
        info_rows.append(f'{row.name},{row.Index + 10}, {row.country3}, {len(new_csv_rows)}, {radius}, {row.degree_increment}, {latinc}, {loninc},' +
                         ','.join([str(e) for e in extremes]))
        csv_rows.extend(new_csv_rows)
    with open('testoutsep.csv', 'w') as fh:
        fh.write('\n'.join(csv_rows))
    with open('grid_info.csv', 'w') as fh:
        fh.write('\n'.join(info_rows))

if __name__ == "__main__":
    fdir = '~/data/techsalerator/raw/'
    make_poi_grids(fdir + 'POIpolygons.csv')
