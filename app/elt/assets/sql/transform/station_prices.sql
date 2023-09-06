SELECT 	s.brandid AS brand_id, s.stationid AS station_id, s.brand, s.code AS station_code, s.name AS station_name,
		s.address AS full_address, replace(s.location,'''','"')::jsonb->>'latitude' AS latitude,
		replace(s.location,'''','"')::jsonb->>'longitude' AS longitude,
		pr.fueltype AS fuel_type, pr.price AS price_aud, pr.lastupdated AS price_last_updated
FROM stations s
LEFT JOIN prices pr
	ON CAST(s.code as INT) = pr.stationcode