package timeutil

import "time"

func Now() time.Time {
	return time.Now().UTC()
}

func HoursUntil(t time.Time) float64 {
	return time.Until(t).Hours()
}
