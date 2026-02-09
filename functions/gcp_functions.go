package functions

import (
	"encoding/json"
	"math"
	"net/http"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
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

func init() {
	functions.HTTP("Average", Average)
}

func Average(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Use POST", http.StatusMethodNotAllowed)
		return
	}

	var req AvgRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	avg, ok := averageLatLngSpherical(req.Points)
	if !ok {
		http.Error(w, "Invalid Points", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(AvgResponse{
		Lat:    avg.Lat,
		Lng:    avg.Lng,
		Method: "spherical",
	})
}

func averageLatLngSpherical(points []Point) (Point, bool) {
	if len(points) != 4 {
		return Point{}, false
	}

	var x, y, z float64
	for _, p := range points {
		if p.Lat < -90 || p.Lat > 90 || p.Lng < -180 || p.Lng > 180 {
			return Point{}, false
		}
		lat := p.Lat * math.Pi / 180
		lng := p.Lng * math.Pi / 180
		clat := math.Cos(lat)

		x += clat * math.Cos(lng)
		y += clat * math.Sin(lng)
		z += math.Sin(lat)
	}

	x /= 4
	y /= 4
	z /= 4

	lng := math.Atan2(y, x)
	hyp := math.Sqrt(x*x + y*y)
	lat := math.Atan2(z, hyp)

	return Point{Lat: lat * 180 / math.Pi, Lng: lng * 180 / math.Pi}, true
}
