package main

import (
	"math"

	"github.com/gogearbox/gearbox"
)

type Point struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

type AvgRequest struct {
	Points []Point `json:"points"`
}

type AvgResponse struct {
	Lat    float64 `json:"lat"`
	Lng    float64 `json:"lng"`
	Method string  `json:"method"`
}

func AverageLatLngSpherical(points []Point) (Point, bool) {
	if len(points) != 4 {
		return Point{}, false
	}

	var x, y, z float64
	for _, p := range points {
		if p.Lat < -90 || p.Lat > 90 || p.Lng < -180 || p.Lng > 180 {
			return Point{}, false
		}

		lat := p.Lat * math.Pi / 180.0
		lng := p.Lng * math.Pi / 180.0

		clat := math.Cos(lat)
		x += clat * math.Cos(lng)
		y += clat * math.Sin(lng)
		z += math.Sin(lat)
	}

	x /= 4.0
	y /= 4.0
	z /= 4.0

	lng := math.Atan2(y, x)
	hyp := math.Sqrt(x*x + y*y)
	lat := math.Atan2(z, hyp)

	return Point{
		Lat: lat * 180.0 / math.Pi,
		Lng: lng * 180.0 / math.Pi,
	}, true
}

func AverageLatLngSimple(points []Point) (Point, bool) {
	if len(points) != 4 {
		return Point{}, false
	}

	var latSum, lngSum float64
	for _, p := range points {
		if p.Lat < -90 || p.Lat > 90 || p.Lng < -180 || p.Lng > 180 {
			return Point{}, false
		}
		latSum += p.Lat
		lngSum += p.Lng
	}

	return Point{Lat: latSum / 4.0, Lng: lngSum / 4.0}, true
}

func main() {
	gb := gearbox.New()

	gb.Post("/geo_average", func(ctx gearbox.Context) {
		var req AvgRequest
		if err := ctx.ParseBody(&req); err != nil {
			ctx.Status(gearbox.StatusBadRequest).SendString("Invalid JSON body")
			return
		}

		avg, ok := AverageLatLngSpherical(req.Points)
		if !ok {
			ctx.Status(gearbox.StatusBadRequest).SendString("Invalid points")
			return
		}

		_ = ctx.SendJSON(AvgResponse{
			Lat:    avg.Lat,
			Lng:    avg.Lng,
			Method: "spherical",
		})
	})

	_ = gb.Start(":8080")
}
