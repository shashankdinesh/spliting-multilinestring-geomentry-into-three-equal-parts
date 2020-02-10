import ast
from math import radians,sin,cos,asin,sqrt
import ogr,psycopg2,json

def haversine( pointA, pointB):
 if (type(pointA) != tuple) or (type(pointB) != tuple):
  raise TypeError("Only tuples are supported as arguments")
 lat1 = pointA[1]
 lon1 = pointA[0]
 lat2 = pointB[1]
 lon2 = pointB[0]
 # convert decimal degrees to radians
 lat1, lon1, lat2, lon2 = map(radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
 # haversine formula
 dlon = lon2 - lon1
 dlat = lat2 - lat1
 a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
 c = 2 * asin(sqrt(a))
 r = 6371  # Radius of earth in kilometers. Use 3956 for miles
 # returns result in kilometer
 return c * r

conn = psycopg2.connect(database="tech", user="postgres", password="dudes11081991", host="localhost", port="5432")
print("Opened database successfully")
cur = conn.cursor()
cur.execute("SELECT assetguid_start, assetguid_end, route, distance, start_coord, end_coord FROM network_sch.route_u_2_u_fl")
cols = cur.fetchall()
for iuy in cols:
    cur.execute("SELECT annual_val FROM network_sch.ex_org WHERE assetguid='%s'"%iuy[0])
    try:
        assetguid_start_value = int(cur.fetchall()[0][0])
    except:
        continue

    cur.execute("SELECT annual_val FROM network_sch.ex_org WHERE assetguid='%s'" % iuy[1])
    print(cur.fetchall())
    try:
        assetguid_end_value = int(cur.fetchall()[0][0])
    except:
        continue
    length = float(iuy[3])*100000
    a1= ast.literal_eval(iuy[2])
    checkpoint = ast.literal_eval(iuy[4])["coordinates"]
    a1_2 =ast.literal_eval(iuy[5])["coordinates"]
    finallist = []
    x = True

    def lineiter(checkpoint, finallist):
        ght, fht = {}, {}
        eht, efht = {}, {}
        for i in range(len(a1["coordinates"])):
            if a1["coordinates"][i] not in finallist and a1["coordinates"][i][::-1] not in finallist:
                startpoint = a1["coordinates"][i][0]
                eht[i] = a1["coordinates"][i][len(a1["coordinates"][i]) - 1]
                ght[haversine(tuple(checkpoint), tuple(startpoint))] = i
                fht[haversine(tuple(checkpoint), tuple(a1["coordinates"][i][len(a1["coordinates"][i]) - 1]))] = i
                efht[i] = a1["coordinates"][i][0]
        return ght, eht, fht, efht


    while x:
        ght, eht, fht, efht = lineiter(checkpoint, finallist)
        if len(ght) > 0:
            if min(ght.keys()) < min(fht.keys()):
                iota = ght[min(ght.keys())]
                finallist.append(a1["coordinates"][iota])
                checkpoint = eht[iota]
            else:
                iota = fht[min(fht.keys())]
                finallist.append(a1["coordinates"][iota][::-1])
                checkpoint = efht[iota]

        else:
            x = False

    a2 = {"type": "MultiLineString", "coordinates": finallist}
    coord_d_1 = {"type": "MultiLineString", "coordinates": []}
    coord_d_2 = {"type": "MultiLineString", "coordinates": []}
    coord_d_3 = {"type": "MultiLineString", "coordinates": []}
    di = 0

    for i in range(len(a2["coordinates"])):
        for j in range(len(a2["coordinates"][i]) - 1):
            if di < ((assetguid_start_value * length * 2) / (3 * (assetguid_start_value + assetguid_end_value))):
                coord_d_1["coordinates"].append([list(a2["coordinates"][i][j]), list(a2["coordinates"][i][j + 1])])
                print(haversine(tuple(a2["coordinates"][i][j]), tuple(a2["coordinates"][i][j + 1])) * 1000)
                di = di + haversine(tuple(a2["coordinates"][i][j]), tuple(a2["coordinates"][i][j + 1])) * 1000

            elif di > ((assetguid_start_value * length * 2) / (3 * (assetguid_start_value + assetguid_end_value))) and di < (
                    ((assetguid_start_value * length * 2) / (3 * (assetguid_start_value + assetguid_end_value))) + (length / 3)):

                coord_d_2["coordinates"].append([list(a2["coordinates"][i][j]), list(a2["coordinates"][i][j + 1])])
                print(haversine(tuple(a2["coordinates"][i][j]), tuple(a2["coordinates"][i][j + 1])) * 1000)
                di = di + haversine(tuple(a2["coordinates"][i][j]), tuple(a2["coordinates"][i][j + 1])) * 1000

            elif di >= (((assetguid_start_value * length * 2) / (3 * (assetguid_start_value + assetguid_end_value))) + (length / 3)):

                coord_d_3["coordinates"].append([list(a2["coordinates"][i][j]), list(a2["coordinates"][i][j + 1])])
                print(haversine(tuple(a2["coordinates"][i][j]), tuple(a2["coordinates"][i][j + 1])) * 1000)
                di = di + haversine(tuple(a2["coordinates"][i][j]), tuple(a2["coordinates"][i][j + 1])) * 1000

    print(json.dumps(coord_d_1), '\n', json.dumps(coord_d_2), '\n', json.dumps(coord_d_3))

    sta_1_start = coord_d_1["coordinates"][0][0]
    lastline_1 = len(coord_d_1["coordinates"]) - 1
    lastcord_1 = len(coord_d_1["coordinates"][lastline_1]) - 1
    sta_1_end = coord_d_1["coordinates"][lastline_1][lastcord_1]
    point_1 = ogr.Geometry(ogr.wkbPoint)
    point_1.AddPoint((sta_1_start[0] + sta_1_end[0]) / 2, (sta_1_start[1] + sta_1_end[1]) / 2)
    buff_1 = point_1.Buffer(
        .95 * (haversine((sta_1_start[0], sta_1_start[1]), (sta_1_end[0], sta_1_end[1])) / 200)).ExportToJson()



    sta_2_start = coord_d_2["coordinates"][0][0]
    lastline_2 = len(coord_d_2["coordinates"]) - 1
    lastcord_2 = len(coord_d_2["coordinates"][lastline_2]) - 1
    sta_2_end = coord_d_2["coordinates"][lastline_2][lastcord_2]
    point_2 = ogr.Geometry(ogr.wkbPoint)
    point_2.AddPoint((sta_2_start[0] + sta_2_end[0]) / 2, (sta_2_start[1] + sta_2_end[1]) / 2)
    buff_2 = point_2.Buffer(
        .95 * (haversine((sta_2_start[0], sta_2_start[1]), (sta_2_end[0], sta_2_end[1])) / 200)).ExportToJson()



    sta_3_start = coord_d_3["coordinates"][0][0]
    lastline_3 = len(coord_d_3["coordinates"]) - 1
    lastcord_3 = len(coord_d_3["coordinates"][lastline_3]) - 1
    sta_3_end = coord_d_3["coordinates"][lastline_3][lastcord_3]
    point_3 = ogr.Geometry(ogr.wkbPoint)
    point_3.AddPoint((sta_3_start[0] + sta_3_end[0]) / 2, (sta_3_start[1] + sta_3_end[1]) / 2)
    buff_3 = point_3.Buffer(
        .95 * (haversine((sta_3_start[0], sta_3_start[1]), (sta_3_end[0], sta_3_end[1])) / 200)).ExportToJson()

    print(buff_1,'\n',buff_2,'\n',buff_3,'\n',iuy[0],'\n',iuy[1])

    cur.execute("INSERT INTO network_sch.route_u_2_u_fl_buffer(path_start, path_mid, path_end, buffer_start, buffer_mid, "
                "buffer_end, assetguid_start, assetguid_end) VALUES ({},{},{},{},{},{},{},{})".format("'"+str(json.dumps(coord_d_1))+"'",
                "'"+str(json.dumps(coord_d_2))+"'","'" + str(json.dumps(coord_d_3)) + "'","'" + str(buff_1) + "'" , "'"+str(buff_2)+"'",
                "'"+str(buff_3)+"'","'"+str(iuy[0])+"'","'"+str(iuy[1])+"'"))
    conn.commit()




