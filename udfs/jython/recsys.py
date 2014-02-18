from math import exp

@outputSchema("scaled: double")
def logistic_scale(val, logistic_param):
    return -1.0 + 2.0 / (1.0 + exp(-logistic_param * val))

@outputSchema("t: (item_A, item_B, dist: double, raw_weight: double)")
def best_path(paths):
    return sorted(paths, key=lambda t:t[2])[0]
