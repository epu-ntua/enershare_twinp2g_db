import entsog


def greek_operator_point_directions():
    points = entsog.EntsogPandasClient().query_operator_point_directions()
    #points.to_csv('outfile3.csv')
    mask1 = points['t_so_balancing_zone'].str.contains('Greece')
    mask2 = points['t_so_country'].str == 'GR'
    masked_points = points[mask1 | mask2]
    #points[['t_so_balancing_zone', 't_so_country', 'adjacent_country', 'adjacent_zones']].to_csv('outfile_33.csv')
    #masked_points.to_csv('outfile44.csv')
    #print(masked_points)
    keys = []
    for idx, item in masked_points.iterrows():
        keys.append(f"{item['operator_key']}{item['point_key']}{item['direction_key']}")

    return keys


#data = client.query_operational_point_data(start=start, end=end, indicators=['renomination'], point_directions=keys,
   #                                        verbose=False)
