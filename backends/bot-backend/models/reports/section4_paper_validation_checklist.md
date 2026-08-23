# Section 4 Paper Validation Checklist

- [x] IOFS enabled in paper mode only
- [x] IOFS starts in shadow mode
- [x] BTCUSDT and ETHUSDT only
- [x] London 07:00-10:00 UTC and NY 13:00-16:00 UTC sessions only
- [x] IOFS logs score + reason per cycle
- [ ] Every accepted trade reviewed within 24h
- [ ] Organic dataset builder runs nightly at 02:00 UTC
- [ ] Minimum 20 complete closed paper trades collected
- [ ] Minimum 4 calendar weeks completed
- [ ] Win rate >= 58%
- [ ] Break-even stop placed at entry + buffer for every TP1 hit
- [ ] TP1:TP2 ratio below 20:1
- [ ] Conservative profile takes fewer trades per day than aggressive
- [ ] No crash loops
- [ ] No circuit breaker trips from code errors

Status: In Progress. Setup checks above were accepted on 2026-06-13. Outcome
checks remain open until the full paper-validation evidence is collected.
