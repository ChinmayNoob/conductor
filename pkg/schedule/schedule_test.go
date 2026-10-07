package schedule

import (
	"testing"
	"time"
)

func mustParse(t *testing.T, expr, tz string) Spec {
	t.Helper()
	s, err := Parse(expr, tz)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestParse(t *testing.T) {
	for _, expr := range []string{"*/5 * * * *", "0 9 * * MON-FRI", "@hourly", "@every 30s"} {
		if _, err := Parse(expr, "UTC"); err != nil {
			t.Errorf("%q: %v", expr, err)
		}
	}
	for _, expr := range []string{"", "* * *", "61 * * * *", "@sometimes"} {
		if _, err := Parse(expr, "UTC"); err == nil {
			t.Errorf("%q: expected an error", expr)
		}
	}
	if _, err := Parse("@hourly", "Mars/Olympus_Mons"); err == nil {
		t.Error("unknown time zone accepted")
	}
}

func TestNextRespectsTimeZone(t *testing.T) {
	s := mustParse(t, "0 9 * * *", "Asia/Kolkata") // 09:00 IST = 03:30 UTC
	got := s.Next(time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC))
	want := time.Date(2026, 10, 7, 3, 30, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("Next = %v, want %v", got, want)
	}
}

func TestDecideOnTime(t *testing.T) {
	s := mustParse(t, "*/5 * * * *", "UTC")
	due := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	d := Decide(s, Skip, due, due.Add(2*time.Second))
	if !d.Fire || !d.FireAt.Equal(due) || !d.Next.Equal(due.Add(5*time.Minute)) {
		t.Fatalf("decision = %+v", d)
	}
}

func TestDecideMisfirePolicies(t *testing.T) {
	s := mustParse(t, "@hourly", "UTC")
	due := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	now := due.Add(3*time.Hour + 20*time.Minute) // missed 10:00 through 13:00

	skip := Decide(s, Skip, due, now)
	if skip.Fire || !skip.Next.Equal(time.Date(2026, 1, 1, 14, 0, 0, 0, time.UTC)) {
		t.Errorf("skip = %+v; want no fire, next 14:00", skip)
	}

	once := Decide(s, RunOnce, due, now)
	if !once.Fire || !once.Next.Equal(time.Date(2026, 1, 1, 14, 0, 0, 0, time.UTC)) {
		t.Errorf("run_once = %+v; want fire, next 14:00", once)
	}

	// catch_up fires 10, 11, 12 and 13, then is back on schedule.
	var fired []int
	next := due
	for range 10 {
		d := Decide(s, CatchUp, next, now)
		if !d.Fire {
			break
		}
		fired = append(fired, d.FireAt.Hour())
		next = d.Next
		if next.After(now) {
			break
		}
	}
	if len(fired) != 4 || fired[0] != 10 || fired[3] != 13 || next.Hour() != 14 {
		t.Errorf("catch_up fired %v, next %v; want [10 11 12 13], next 14:00", fired, next)
	}
}

func TestDecideSlightlyLateCountsAsOnTime(t *testing.T) {
	s := mustParse(t, "@every 10s", "UTC")
	due := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	d := Decide(s, Skip, due, due.Add(30*time.Second))
	if !d.Fire || !d.Next.After(due.Add(30*time.Second)) {
		t.Fatalf("decision = %+v; want fire with next in the future", d)
	}
}
