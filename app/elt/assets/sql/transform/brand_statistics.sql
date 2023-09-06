select brand, count(distinct fuel_type) no_of_fuel_types, min(price_aud) lowest_price, max(price_aud) highest_price,
		sum(case
			when fuel_type = 'EV' then 1
			else 0
		end) > 0 has_EV
from station_prices
group by brand