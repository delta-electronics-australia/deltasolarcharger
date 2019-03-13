""" This file is for calculating the dynamic buffer curve of the Delta Solar Charger """

import numpy as np
import matplotlib.pyplot as plt


def buffer_calc(value):
    """ Calculates the buffers at a given sd/mean and prints it out """

    if type(value) is not list():
        value = [value]

    p_list = [('aggressive', p_aggressive), ('moderate', p_moderate), ('conservative', p_conservative),
              ('ultraconservative', p_ultaconservative)]
    print(list(value))
    for p in p_list:
        print(p[0], p[1],
              np.polynomial.polynomial.polyval(np.log(value), p[1]))


np.set_printoptions(suppress=True)

# x (input) should be sd(pv_window)/mean(pv_window)
x = np.array([0.0001, 0.001, 0.01, 0.1])
# # y should be the appropriate buffer
y_aggressive = np.array([0, 1, 5, 10])
y_moderate = np.array([0, 3, 8, 20])
y_conservative = np.array([1, 11, 22, 35])
y_ultraconservative = np.array([1, 15, 30, 40])

x_new = np.linspace(x[0], x[-1], num=len(x) * 50)

p_aggressive = np.polynomial.polynomial.polyfit(np.log(x), y_aggressive, deg=2)
ffit_aggressive = np.polynomial.polynomial.polyval(np.log(x_new), p_aggressive)

p_moderate = np.polynomial.polynomial.polyfit(np.log(x), y_moderate, deg=2)
ffit_moderate = np.polynomial.polynomial.polyval(np.log(x_new), p_moderate)
#
p_conservative = np.polynomial.polynomial.polyfit(np.log(x), y_conservative, deg=2)
ffit_conservative = np.polynomial.polynomial.polyval(np.log(x_new), p_conservative)

p_ultaconservative = np.polynomial.polynomial.polyfit(np.log(x), y_ultraconservative, deg=2)
ffit_ultaconservative = np.polynomial.polynomial.polyval(np.log(x_new), p_ultaconservative)

x_new1 = np.linspace(x[0], 0.02, num=len(x) * 50)
y_old = (-200000 * (x_new1 ** 2) + (5000 * x_new1) + 5)

plt.semilogx(x_new, ffit_aggressive, 'r', x_new, ffit_moderate, 'y', x_new, ffit_conservative, 'g', x_new,
             ffit_ultaconservative, 'c', x_new1, y_old, 'b')
plt.legend(['Aggressive', 'Moderate', 'Conservative', 'Ultra Conservative'])
plt.title('Battery Buffering Usage')
plt.ylabel('Buffer (%)')
plt.show()

buffer_calc(np.array([10 ** (-4), 10 ** (-3), 10 ** (-2), 10 ** (-1)]))
