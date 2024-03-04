import entsog


def greek_operator_point_directions():
    points = entsog.EntsogPandasClient().query_operator_point_directions()
    mask1 = points['t_so_balancing_zone'].str.contains('Greece')
    mask2 = points['t_so_country'].str == 'GR'
    masked_points = points[mask1 | mask2]

    keys = []
    for idx, item in masked_points.iterrows():
        keys.append(f"{item['operator_key']}{item['point_key']}{item['direction_key']}")

    return keys

