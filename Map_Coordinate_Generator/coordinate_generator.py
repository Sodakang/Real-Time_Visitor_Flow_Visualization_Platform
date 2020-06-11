import numpy as np
import redis
import matplotlib.pyplot as plt

cords = np.array([
[33.810541, -117.993618],
[33.781315, -117.938347],
[33.744417, -117.867698],
[33.733915, -117.988237],
[33.686842, -117.831782],
[33.685032, -117.911576],
[33.629632, -117.605826],
[33.619136, -117.710371],
[33.698015, -117.764403],
[33.708692, -117.944627]])

print(cords.shape)
cords_x, cords_y = np.hsplit(cords, 2)

max_xy = cords.max(axis=0)
min_xy = cords.min(axis=0)

rx0 = min_xy[0]
rx1 = max_xy[0]
ry0 = min_xy[1]
ry1 = max_xy[1]

center = [(rx0+rx1)/2, (ry0+ry1)/2]
width = rx1 - rx0 + 0.1
height = ry1 - ry0 + 0.1

print(center, width, height)

# define the digital map width and height
w = 100
h = int(height * 100 / width)


# generate a corresponding grid (size: w by h) containing real cords
x_grid = np.linspace(center[0] - width/2, center[0] + width/2, w)
y_grid = np.linspace(center[1] - height/2, center[1] + height/2, h)

X, Y = np.meshgrid(x_grid, y_grid)
grid = np.stack((X, Y), -1)
print("Grid shape:", grid.shape)


# transform cords to the points in our grid
def matchClosestValue(arrInputs, arrValues):
    aa = arrInputs
    bb = arrValues
    idx = np.searchsorted(bb, aa)
    msk = idx > len(bb) - 1
    idx[msk] = len(bb) - 1

    idx_new=np.array([idx[i]-1 if abs(bb[idx[i]-1]-aa[i])<abs(bb[idx[i]]-aa[i]) else idx[i] for i in range(len(idx))])
    aa = np.array([bb[_] for _ in idx_new])
    return aa


cords_x = matchClosestValue(cords_x, x_grid).flatten()
cords_y = matchClosestValue(cords_y, y_grid).flatten()

plt.scatter(X, Y)
plt.scatter(cords_x, cords_y)

# for each base station, calculate all points covered by circle of each radius

rads = [0.01, 0.02, 0.03,0.04,0.05]   # distance = 10000 * latency
result = {} #{"x0,y0" : [[x1,y1], [x2,y2]]}


for i in range(len(cords_x)):
    rad_prev = 0
    for rad in rads:
        x0 = cords_x[i]
        y0 = cords_y[i]
        m = np.square(X-x0) + np.square(Y-y0)
        g = grid[(rad_prev * rad_prev < m) & (m < rad * rad)]
        key = str(x0)+","+str(y0)+":"+str(rad)
        result[key] = g
        rad_prev = rad

# r =redis.Redis(host="54.89.216.134",port=6379)
r =redis.Redis(host="localhost",port=6379)

for key, g in result.items():
    for pair in g:
        value = str(pair[0])+","+str(pair[1])
        r.rpush(key, value)

for i in range(len(cords_x)):
    for rad in rads:
        print('"'+str(cords_x[i])+","+str(cords_y[i])+":"+str(rad)+'",')