select brand, fuel_type, ROUND(avg(price_aud::numeric),2) average_price
from station_prices
group by brand, fuel_type
order by brand, fuel_type