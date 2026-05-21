# Known Data Characteristics & Limitations

This documents the data-quality findings surfaced while building the reliability
marts. These are characterized, not hidden — each is a deliberate decision with a
stated rationale.

## 1. Only the L train populates the feed's `delay_seconds`
Across ~150M raw observations over 10 days, the L train is the **only** line that
populates `delay_seconds` (1.67M non-zero values, including early arrivals). All
29 other routes return exactly 0 across tens of millions of rows. `delay_seconds = 0`
therefore conflates "on time" with "not reported" for every line except the L.
**Decision:** delay is computed independently as `predicted_arrival − scheduled_arrival`
via the schedule join, for all lines. The feed's value is retained only as a
reference field for the L cross-check.

## 2. Computed delay validated against the feed on the L
The schedule-derived delay was validated against the feed's `delay_seconds` on the
L — the only line where both exist. After deduplicating prediction history (see #3),
the computed delay agrees with the feed to a **15-second median**, with 90% of
trips within 30 seconds. This validates the *method*; it is then applied to all
lines, whose inputs (predicted arrival, schedule) are identical in kind. The
extrapolation is stated, not assumed: only the L has ground truth.

## 3. ~134 predictions per arrival (prediction history)
`RAW.TRIP_UPDATES` retains every poll's prediction, ~134 per train-arrival. Raw
aggregates were therefore inflated ~134× and biased toward trains that linger in
the prediction window. **Decision:** the staging layer deduplicates to one row per
(trip, stop, NY service date) — the final prediction before arrival. Counts in the
marts are real arrivals, not predictions.

## 4. Timezone (resolved)
Schedule times are America/New_York wall-clock; predicted arrivals are UTC epoch.
A naïve comparison produced a 4-hour (EDT) systematic offset. **Resolved** via
DST-aware `convert_timezone`, validated by the 15s L agreement above.

## 5. After-midnight (GTFS hour ≥ 24) residual — KNOWN LIMITATION
GTFS encodes post-midnight trips as hour ≥ 24 (e.g. `24:00:00`, `25:14:00`) belonging
to the prior service day. Anchoring the schedule to the service date (not the
predicted timestamp's day) corrected the bulk of a large negative bias on trunk
lines. A **residual** remains on heavy-overnight IRT lines (2/3/5): trips *predicted*
after midnight can still resolve to the wrong service day, leaving a negative skew
of a few tens of seconds in those buckets. **Fully resolving this requires the GTFS
service-day definition (rollover ~3–4am, not midnight); deferred as future work.**
Magnitude is small and bounded; it does not affect daytime buckets.

## 6. Schedule padding — a finding, not an error
Simple high-frequency lines (e.g. the GS / Grand Central Shuttle) show consistent
small negative medians (~−20s) with very low variance and ~100% on-time. This is
not computation error — it reflects **schedule padding**: printed schedules build in
slack, so trains legitimately run slightly ahead of the timetable while being
operationally on time. Reported as-is.

## 7. Sample floor
The reliability mart applies `having count(*) >= 30`. Without it, buckets with a
single arrival (e.g. one late-night train) report that lone trip's value as a
"median," producing spurious extremes. Thin buckets are excluded, not hidden.

---
**Net:** delay is computed (the feed only reports it for one line), validated to 15s
on the line where validation is possible, deduplicated to real arrivals, and
timezone- and midnight-corrected. Two residual characteristics (after-midnight IRT
skew, shuttle padding) are documented rather than masked. Cross-line comparison is
sound at the day-type / time-period granularity these marts operate on.
